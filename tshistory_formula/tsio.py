from collections import defaultdict
from datetime import timedelta
import hashlib
import itertools
import json

import pandas as pd
from psyl.lisp import parse, serialize
from tshistory.tsio import timeseries as basets
from tshistory.util import (
    diff,
    tx
)

from tshistory_formula import funcs, gfuncs  # trigger registration
from tshistory_formula import (
    api,  # trigger extension
    interpreter,
    helper
)
from tshistory_formula.registry import (
    FINDERS,
    FUNCS,
    HISTORY,
    IDATES,
    METAS,
    GFINDERS,
    GAUTO,
    GIDATES,
    GMETAS
)


class timeseries(basets):
    fast_staircase_operators = set(['+', '*', 'series', 'add', 'priority'])
    metadata_compat_excluded = ()
    concurrency = 16

    def find_series(self, cn, tree):
        op = tree[0]
        finder = FINDERS.get(op)
        seriestree = finder(cn, self, tree) if finder else {}
        for item in tree:
            if isinstance(item, list):
                seriestree.update(
                    self.find_series(cn, item)
                )
        return seriestree

    def find_metas(self, cn, tree):
        op = tree[0]
        metas = METAS.get(op)
        seriesmeta = metas(cn, self, tree) if metas else {}
        for item in tree:
            if isinstance(item, list):
                seriesmeta.update(
                    self.find_metas(cn, item)
                )
        return seriesmeta

    def find_callsites(self, cn, operator, tree):
        op = tree[0]
        sites = []
        if op == operator:
            sites.append(tree)
        for item in tree:
            if isinstance(item, list):
                sites.extend(
                    self.find_callsites(cn, operator, item)
                )
        return sites

    def find_operators(self, cn, tree):
        ops = {
            tree[0]: FUNCS.get(tree[0])
        }
        for item in tree:
            if isinstance(item, list):
                newops = self.find_operators(cn, item)
                ops.update(newops)
        return ops

    def has_asof(self, cn, tree):
        op = tree[0]

        if op == 'asof':
            return True

        for item in tree[1:]:
            if isinstance(item, list):
                if self.has_asof(cn, item):
                    return True

        return False

    def check_tz_compatibility(self, cn, tree):
        """check that series are timezone-compatible
        """

        def find_meta(tree, tzstatus, path=()):
            op = tree[0]
            path = path + (op,)
            metas = METAS.get(op)
            if metas:
                for name, metadata in metas(cn, self, tree).items():
                    tzaware = metadata['tzaware'] if metadata else None
                    if 'naive' in path:
                        tzaware = False
                    tzstatus[(name, path)] = tzaware
            for item in tree:
                if isinstance(item, list):
                    find_meta(item, tzstatus, path)

        metamap = {}
        find_meta(tree, metamap)
        if not metamap:
            return {}

        def tzlabel(status):
            if status is None: return 'unknown'
            return 'tzaware' if status else 'tznaive'

        first_tzaware = next(iter(metamap.values()))
        for (name, path), tzaware in metamap.items():
            if first_tzaware != tzaware:
                raise ValueError(
                    f'Formula `{name}` has tzaware vs tznaive series:'
                    f'{",".join("`%s:%s`" % (k, tzlabel(v)) for k, v in metamap.items())}'
                )
        return first_tzaware

    def formula_stats(self, cn, name):
        autos = helper.find_autos(cn, self, name)
        nodes = helper.scan_descendant_nodes(cn, self, name)
        nodes.update({'autotrophics': autos})
        return nodes

    @tx
    def register_dependants(self, cn, name, tree):
        cn.execute(
            f'delete from "{self.namespace}".dependant where '
            f'sid in (select id from "{self.namespace}".formula where name = %(name)s)',
            name=name
        )
        for dep in self.find_series(cn, tree):
            if self.type(cn, dep) != 'formula':
                continue
            cn.execute(
                f'insert into "{self.namespace}".dependant '
                f'(sid, needs) '
                f'values ('
                f' (select id from "{self.namespace}".formula where name = %(name)s),'
                f' (select id from "{self.namespace}".formula where name = %(dep)s)'
                f') on conflict do nothing',
                name=name,
                dep=dep
            )

    @tx
    def dependants(self, cn, name, direct=False):
        deps = [
            n for n, in cn.execute(
                f'select f.name '
                f'from "{self.namespace}".formula as f, '
                f'     "{self.namespace}".formula as f2,'
                f'     "{self.namespace}".dependant as d '
                f'where f.id = d.sid and '
                f'      d.needs = f2.id and '
                f'      f2.name = %(name)s',
                name=name
            ).fetchall()
        ]

        if direct:
            return sorted(set(deps))

        for dname in deps[:]:
            deps.extend(
                self.dependants(cn, dname)
            )
        return sorted(set(deps))

    @tx
    def register_formula(self, cn, name, formula, reject_unknown=True):
        assert isinstance(name, str), 'The name must be a string'
        name = name.strip()
        assert len(name), 'The new name must contain non whitespace items'

        if self.exists(cn, name) and self.type(cn, name) == 'primary':
            raise TypeError(
                f'primary series `{name}` cannot be overriden by a formula'
            )

        # basic syntax check
        tree = parse(formula)
        # this normalizes the formula
        formula = serialize(tree)

        # bad operators
        operators = self.find_operators(cn, tree)
        badoperators = [
            op
            for op, func in operators.items()
            if func is None
        ]
        if badoperators:
            raise ValueError(
                f'Formula `{name}` refers to unknown operators '
                f'{", ".join("`%s`" % o for o in badoperators)}'
            )

        # type checking
        i = interpreter.Interpreter(cn, self, {})
        rtype = helper.typecheck(tree, env=i.env)
        if not helper.sametype(rtype, pd.Series):
            raise TypeError(
                f'formula `{name}` must return a `Series`, not `{rtype.__name__}`'
            )

        # build metadata & check compat
        seriesmeta = self.find_metas(cn, tree)
        if not all(seriesmeta.values()) and reject_unknown:
            badseries = [k for k, v in seriesmeta.items() if not v]
            raise ValueError(
                f'Formula `{name}` refers to unknown series '
                f'{", ".join("`%s`" % s for s in badseries)}'
            )

        tzaware = self.check_tz_compatibility(cn, tree)
        ch = hashlib.sha1(
            serialize(
                self._expanded_formula(cn, formula)
            ).encode()
        ).hexdigest()
        sql = (f'insert into "{self.namespace}".formula '
               '(name, text, contenthash) '
               'values (%(name)s, %(text)s, %(contenthash)s) '
               'on conflict (name) do update '
               'set text = %(text)s, contenthash=%(contenthash)s')
        cn.execute(
            sql,
            name=name,
            text=formula,
            contenthash=ch
        )

        self.register_dependants(cn, name, tree)

        # save metadata
        if tzaware is None:
            # bad situation ...
            return

        coremeta = self.default_meta(tzaware)
        meta = self.internal_metadata(cn, name) or {}
        meta = dict(meta, **coremeta)
        self.update_internal_metadata(cn, name, meta)

    def live_content_hash(self, cn, name):
        return hashlib.sha1(
            serialize(
                self._expanded_formula(
                    cn,
                    self.formula(cn, name)
                )
            ).encode()
        ).hexdigest()

    def default_meta(self, tzaware):
        if tzaware:
            return {
                'tzaware': True,
                'index_type': 'datetime64[ns, UTC]',
                'value_type': 'float64',
                'index_dtype': '|M8[ns]',
                'value_dtype': '<f8'
            }
        return {
            'index_dtype': '<M8[ns]',
            'index_type': 'datetime64[ns]',
            'tzaware': False,
            'value_dtype': '<f8',
            'value_type': 'float64'
        }

    def content_hash(self, cn, name):
        return cn.execute(
            f'select contenthash from "{self.namespace}".formula '
            f'where name=%(name)s',
            name=name
        ).scalar()

    def formula(self, cn, name):
        formula = cn.execute(
            f'select text from "{self.namespace}".formula where name = %(name)s',
            name=name
        ).scalar()
        return formula

    def list_series(self, cn):
        series = super().list_series(cn)
        sql = f'select name from "{self.namespace}".formula'
        series.update({
            name: 'formula'
            for name, in cn.execute(sql)
        })
        return series

    def type(self, cn, name):
        if self.formula(cn, name):
            return 'formula'

        return super().type(cn, name)

    def exists(self, cn, name):
        return super().exists(cn, name) or bool(self.formula(cn, name))

    def update(self, cn, updatets, name, author, **k):
        if self.type(cn, name) == 'formula':
            raise ValueError(f'`{name}` is a formula, it cannot be updated')

        return super().update(cn, updatets, name, author, **k)

    @tx
    def get(self, cn, name, **kw):
        formula = self.formula(cn, name)
        if formula:
            ts = self.eval_formula(cn, formula, **kw)
            if ts is not None:
                ts.name = name
            return ts

        ts = super().get(cn, name, **kw)
        if ts is None and self.othersources:
            ts = self.othersources.get(
                name, **kw
            )

        return ts

    def eval_formula(self, cn, formula, **kw):
        i = kw.get('__interpreter__') or interpreter.Interpreter(cn, self, kw)
        ts = i.evaluate(
            self._expanded_formula(cn, formula, qargs=kw)
        )
        return ts

    def _expanded_formula(self, cn, formula, stopnames=(), qargs=None):
        exp = helper.expanded(
            self,
            cn,
            parse(formula),
            stopnames=stopnames,
            scopes=qargs is not None
        )
        if qargs is not None:
            return helper.inject_toplevel_bindings(
                exp,
                qargs
            )
        return exp

    def expanded_formula(self, cn, name, stopnames=(), **kw):
        formula = self.formula(cn, name)
        if formula is None:
            return

        tree = self._expanded_formula(cn, formula, stopnames, kw)
        if tree is None:
            return

        return serialize(tree)

    @tx
    def delete(self, cn, name):
        if self.type(cn, name) != 'formula':
            return super().delete(cn, name)

        cn.execute(
            f'delete from "{self.namespace}".formula '
            'where name = %(name)s',
            name=name
        )

    @tx
    def iter_revisions(
            self, cn, name,
            from_value_date=None,
            to_value_date=None,
            from_insertion_date=None,
            to_insertion_date=None,
            **kw):
        idates = self.insertion_dates(
            cn, name,
            from_insertion_date=from_insertion_date,
            to_insertion_date=to_insertion_date
        )
        for idate in idates:
            yield idate, self.get(
                cn, name,
                revision_date=idate,
                from_value_date=from_value_date,
                to_value_date=to_value_date
            )

    def _custom_history_sites(self, cn, tree):
        return [
            call
            for sname in HISTORY
            for call in self.find_callsites(cn, sname, tree)
        ]

    def _custom_idates_sites(self, cn, tree):
        return [
            call
            for sname in IDATES
            for call in self.find_callsites(cn, sname, tree)
        ]

    def _auto_history(self, cn, tree,
                      from_insertion_date=None,
                      to_insertion_date=None,
                      from_value_date=None,
                      to_value_date=None,
                      diffmode=False,
                      _keep_nans=False,
                      **kw):
        assert tree
        i = interpreter.OperatorHistory(
            cn, self, {
                'from_value_date': from_value_date,
                'to_value_date': to_value_date,
                'from_insertion_date': from_insertion_date,
                'to_insertion_date': to_insertion_date,
                'diffmode': diffmode,
                '_keep_nans': _keep_nans
            }
        )
        return i.evaluate_history(tree)

    def _precompute_auto_histories(
            self, cn, hi, basetree,
            from_insertion_date=None,
            to_insertion_date=None,
            from_value_date=None,
            to_value_date=None,
            diffmode=False,
            _keep_nans=False,
            **kw):
        """
        Path of precomputation of the autotrophic operators histories.

        Two notable aspects there:
        * the embedded autotrophic operators are not associated with a
          name (that helps anchoring the results), so we must forge
          one
        * we store the final histories into the .histories of the
          history interpreter (pure side effect)

        """
        trees = self._custom_history_sites(cn, basetree)
        for idx, tree in enumerate(trees):
            chist = self._auto_history(
                cn,
                tree,
                from_insertion_date=from_insertion_date,
                to_insertion_date=to_insertion_date,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                **kw
            ) or {}
            cname = helper.name_of_expr(tree)
            hi.namecache[serialize(tree)] = cname
            hi.histories.update({
                cname: chist
            })


    def _complete_histories_start(
            self, cn, histmap, tree,
            from_value_date=None,
            to_value_date=None,
            **kw):
        """
        Complete the potentially missing entries of the collected histories.

        Takes an `history map` and returns an `history map` with
        possibly more entries.

        Indeed, when `from_insertion_date` is provided to .history, we
        can have this situation:

        #    ^  h0      h1
        #    |
        #  i2| xxx     xxx
        #  i1| xxx
        #  i0| xxx     xxx

        Here, we have three insertion dates (i0, i1, i2), but while
        history of the first series `h0` has values for all the
        idates, the history of the second series `h1` has a gap.

        If we get asked for the formula history starting from `i1`, we
        actually want `h1` to contain something for `i1` also, and
        only by digging further in the past can we provide it.

        """
        mins = [
            min(hist.keys())
            for hist in histmap.values()
            if len(hist)
        ]
        if not len(mins):
            return histmap

        mindate = min(mins)
        for name, hist in histmap.items():
            if mindate not in hist:
                ts_mindate = self.get(
                    cn,
                    name,
                    revision_date=mindate,
                    from_value_date=from_value_date,
                    to_value_date=to_value_date,
                    **kw
                )
                if ts_mindate is not None and len(ts_mindate):
                    # the history must be ordered by key
                    base = {mindate: ts_mindate}
                    base.update(hist)
                    histmap[name] = base

        return histmap


    def _history_diffs(
            self,
            cn, name, hist, idates,
            from_value_date=None,
            to_value_date=None,
            **kw):
        """
        Computes the diff mode of an history.

        This is needed to honor the `diffmode` parameter of .history.

        """
        iteridates = iter(idates)
        firstidate = next(iteridates)
        basets = self.get(
            cn,
            name,
            from_value_date=from_value_date,
            to_value_date=to_value_date,
            revision_date=firstidate - timedelta(seconds=1),
            **kw
        )
        dhist = {}
        for idate in idates:
            dhist[idate] = diff(basets, hist[idate])
            basets = hist[idate]

        return dhist

    @tx
    def history(self, cn, name,
                from_insertion_date=None,
                to_insertion_date=None,
                from_value_date=None,
                to_value_date=None,
                diffmode=False,
                _keep_nans=False,
                **kw):

        if self.type(cn, name) != 'formula':
            hist = super().history(
                cn, name,
                from_insertion_date=from_insertion_date,
                to_insertion_date=to_insertion_date,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                diffmode=diffmode,
                _keep_nans=_keep_nans,
                **kw
            )

            # alternative source ?
            if hist is None and self.othersources:
                hist = self.othersources.history(
                    name,
                    from_value_date=from_value_date,
                    to_value_date=to_value_date,
                    from_insertion_date=from_insertion_date,
                    to_insertion_date=to_insertion_date,
                    _keep_nans=_keep_nans,
                    **kw
                )
            return hist

        formula = self.formula(cn, name)
        tree = self._expanded_formula(
            cn, formula,
            qargs={
                'from_value_date': from_value_date,
                'to_value_date': to_value_date
            }
        )

        if self.has_asof(cn, tree):
            # formula with an "asof" expression
            # in this case we completely switch to the simpler
            # (but potentially slower) path, using iter_revisions
            # in this mode, the "asof" logic is already taken care by
            # the asof rewriter + __revision_date__ parameter in series
            # and other auto operators
            hist = {
                idate: value
                for idate, value in self.iter_revisions(
                        cn, name,
                        from_value_date=from_value_date,
                        to_value_date=to_value_date,
                        from_insertion_date=from_insertion_date,
                        to_insertion_date=to_insertion_date,
                        **kw
                )
            }
            if diffmode:
                return self._history_diffs(
                    cn, name, hist, idates,
                    from_value_date=None,
                    to_value_date=None,
                    **kw)
            return hist

        # normal history: compute the union of the histories
        # of all underlying series
        series = self.find_series(cn, tree)
        histmap = {
            name: self.history(
                cn, name,
                from_insertion_date=from_insertion_date,
                to_insertion_date=to_insertion_date,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                **kw
            ) or {}
            for name in series
        }

        # complete the history with a value for the first idate
        # (we might be missing this because of the query from_insertion_date)
        if histmap and from_insertion_date:
            histmap = self._complete_histories_start(
                cn, histmap, tree,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                **kw
            )

        # prepare the history interpreter using the histories
        # collected so far
        hi = interpreter.HistoryInterpreter(
            name, cn, self, {
                'from_value_date': from_value_date,
                'to_value_date': to_value_date
            },
            histories=histmap
        )

        # delegate work for the autotrophic operator histories
        # this was not done in the previous step because
        # auto operators have their own full-blown protocol to deal
        # with histories
        self._precompute_auto_histories(
            cn, hi, tree,
            from_insertion_date=from_insertion_date,
            to_insertion_date=to_insertion_date,
            from_value_date=from_value_date,
            to_value_date=to_value_date,
            **kw
        )

        # evaluate the formula using the prepared histories
        idates = sorted({
            idate
            for hist in histmap.values()
            for idate in hist
        })

        # build the final history dict
        hist = {
            idate: hi.evaluate(tree, idate, name)
            for idate in idates
        }

        if diffmode and idates:
            hist = self._history_diffs(
                cn, name, hist, idates,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                **kw
            )

        return hist

    @tx
    def insertion_dates(self, cn, name,
                        from_insertion_date=None,
                        to_insertion_date=None,
                        from_value_date=None,
                        to_value_date=None,
                        **kw):
        if self.type(cn, name) != 'formula':
            return super().insertion_dates(
                cn, name,
                from_insertion_date=from_insertion_date,
                to_insertion_date=to_insertion_date,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                **kw
            )

        formula = self.formula(cn, name)
        tree = parse(formula)
        series = self.find_series(cn, tree)
        allrevs = []
        for name in series:
            if not self.exists(cn, name):
                if self.othersources:
                    allrevs += self.othersources.insertion_dates(
                        name,
                        from_insertion_date=from_insertion_date,
                        to_insertion_date=to_insertion_date,
                        from_value_date=from_value_date,
                        to_value_date=to_value_date,
                        **kw
                    )
                continue
            if self.formula(cn, name):
                allrevs += self.insertion_dates(
                    cn, name,
                    from_insertion_date=from_insertion_date,
                    to_insertion_date=to_insertion_date,
                    from_value_date=from_value_date,
                    to_value_date=to_value_date,
                    **kw
                )
            else:
                allrevs += [
                    idate
                    for _id, idate in self._revisions(
                            cn, name,
                            from_insertion_date=from_insertion_date,
                            to_insertion_date=to_insertion_date,
                            from_value_date=from_value_date,
                            to_value_date=to_value_date
                    )]

        # autotrophic operators
        isites = self._custom_idates_sites(cn, tree)
        for site in isites:
            fname = site[0]
            idates_func = IDATES[fname]
            revs = idates_func(
                cn, self, site,
                from_insertion_date,
                to_insertion_date,
                from_value_date,
                to_value_date
            )
            if revs:
                allrevs += revs

        # last resort: get the idates from a full history
        # not great wrt performance ...
        for site in self._custom_history_sites(cn, tree):
            if site in isites:
                continue  # we're already good
            hist = self._auto_history(
                cn,
                site,
                from_insertion_date=from_insertion_date,
                to_insertion_date=to_insertion_date,
                from_value_date=from_value_date,
                to_value_date=to_value_date
            )
            if hist:
                allrevs += list(hist.keys())

        # /auto

        return sorted(set(allrevs))

    @tx
    def latest_insertion_date(self, cn, name):
        formula = self.formula(cn, name)
        if formula:
            return self.insertion_dates(cn, name)[-1]

        return super().latest_insertion_date(cn, name)

    @tx
    def first_insertion_date(self, cn, name):
        formula = self.formula(cn, name)
        if formula:
            return self.insertion_dates(cn, name)[0]

        return super().first_insertion_date(cn, name)


    @tx
    def staircase(self, cn, name, delta,
                  from_value_date=None,
                  to_value_date=None):
        formula = self.formula(cn, name)
        if formula:
            if interpreter.has_compatible_operators(
                    cn, self,
                    parse(formula),
                    self.fast_staircase_operators):
                # go fast
                return self.get(
                    cn, name,
                    from_value_date=from_value_date,
                    to_value_date=to_value_date,
                    __interpreter__=interpreter.FastStaircaseInterpreter(
                        cn, self,
                        {'from_value_date': from_value_date,
                         'to_value_date': to_value_date},
                        delta
                    )
                )

        return super().staircase(
            cn, name, delta,
            from_value_date,
            to_value_date
        )

    @tx
    def internal_metadata(self, cn, name):
        if self.type(cn, name) != 'formula':
            return super().internal_metadata(cn, name)

        if name in cn.cache['internal_metadata']:
            return cn.cache['internal_metadata'][name]
        meta = cn.cache['internal_metadata'][name] = cn.execute(
            f'select internal_metadata '
            f'from "{self.namespace}".formula '
            f'where name = %(name)s',
            name=name
        ).scalar()
        return meta

    @tx
    def update_internal_metadata(self, cn, name, metadata):
        if self.type(cn, name) != 'formula':
            return super().update_internal_metadata(cn, name, metadata)

        imeta = self.internal_metadata(cn, name) or {}
        imeta.update(metadata)
        cn.execute(
            f'update "{self.namespace}".formula '
            'set internal_metadata = %(metadata)s '
            'where name = %(seriesname)s',
            metadata=json.dumps(imeta),
            seriesname=name
        )

    @tx
    def metadata(self, cn, name):
        """Return metadata dict of timeserie."""
        if self.type(cn, name) != 'formula':
            return super().metadata(cn, name)

        sql = (f'select metadata from "{self.namespace}".formula '
               'where name = %(name)s')
        meta = cn.execute(sql, name=name).scalar() or {}
        return meta

    @tx
    def update_metadata(self, cn, name, metadata):
        if self.type(cn, name) != 'formula':
            return super().update_metadata(cn, name, metadata)

        assert isinstance(metadata, dict)
        meta = self.metadata(cn, name) or {}
        newmeta = {
            key: meta[key]
            for key in self.metakeys
            if meta.get(key) is not None
            and key not in self.metadata_compat_excluded
        }
        newmeta.update(metadata)
        sql = (f'update "{self.namespace}".formula '
               'set metadata = %(metadata)s '
               'where name = %(name)s')
        cn.execute(
            sql,
            metadata=json.dumps(newmeta),
            name=name
        )

    @tx
    def log(self, cn, name, **kw):
        if self.formula(cn, name):
            if self.cache.exists(cn, name):
                return self.cache.log(
                    cn, name, **kw
                )
            return []

        return super().log(cn, name, **kw)

    @tx
    def rename(self, cn, oldname, newname):
        assert isinstance(oldname, str), 'The old name must be a string'
        assert isinstance(newname, str), 'The new name must be a string'
        newname = newname.strip()
        assert len(newname), 'The new name must contain non whitespace items'
        # read all formulas and parse them ...
        formulas = cn.execute(
            f'select name, text from "{self.namespace}".formula'
        ).fetchall()
        errors = []

        def edit(tree, oldname, newname):
            newtree = []
            series = False
            for node in tree:
                if isinstance(node, list):
                    newtree.append(edit(node, oldname, newname))
                    continue
                if node == 'series':
                    series = True
                    newtree.append(node)
                    continue
                elif node == oldname and series:
                    node = newname
                newtree.append(node)
                series = False
            return newtree

        for fname, text in formulas:
            tree = parse(text)
            series = self.find_series(
                cn,
                tree
            )
            if newname in series:
                errors.append(fname)
            if oldname not in series or errors:
                continue

            newtree = edit(tree, oldname, newname)
            newtext = serialize(newtree)
            sql = (f'update "{self.namespace}".formula '
                   'set text = %(text)s '
                   'where name = %(name)s')
            cn.execute(
                sql,
                text=newtext,
                name=fname
            )

        if errors:
            raise ValueError(
                f'new name is already referenced by `{",".join(errors)}`'
            )

        if self.type(cn, oldname) == 'formula':
            cn.execute(
                f'update "{self.namespace}".formula '
                'set name = %(newname)s '
                'where name = %(oldname)s',
                oldname=oldname,
                newname=newname
            )
        else:
            super().rename(cn, oldname, newname)

    # groups

    @tx
    def group_type(self, cn, name):
        if self.group_formula(cn, name) is not None:
            return 'formula'
        if self.bindings_for(cn, name):
            return 'bound'
        return super().group_type(cn, name)

    @tx
    def group_exists(self, cn, name):
        kind = self.group_type(cn, name)
        if kind == 'primary':
            return super().group_exists(cn, name)
        assert kind in ('formula', 'bound')
        return True

    @tx
    def group_metadata(self, cn, name):
        kind = self.group_type(cn, name)
        if kind == 'primary':
            return super().group_metadata(cn, name)

        if kind == 'formula':
            table, col = 'group_formula', 'name'
        else:
            assert kind == 'bound'
            table, col = 'group_binding', 'groupname'

        meta = cn.execute(
            f'select metadata from "{self.namespace}".{table} '
            f'where {col} = %(name)s',
            name=name
        ).scalar()

        return meta

    @tx
    def update_group_metadata(self, cn, name, metadata, internal=True):
        kind = self.group_type(cn, name)
        if kind == 'primary':
            return super().update_group_metadata(cn, name, metadata, internal)

        if kind == 'formula':
            table, col = 'group_formula', 'name'
        else:
            assert kind == 'bound'
            table, col = 'group_binding', 'groupname'

        meta = self.group_metadata(cn, name) or {}
        # remove al but internal stuff
        newmeta = {
            key: meta[key]
            for key in self.metakeys
            if meta.get(key) is not None
        }
        newmeta.update(metadata)

        sql = (
            f'update "{self.namespace}".{table} '
            'set metadata = %(metadata)s '
            f'where {col} = %(name)s'
        )
        cn.execute(
            sql,
            metadata=json.dumps(newmeta),
            name=name
        )

    def check_group_tz_compatibility(self, cn, tree):
        """check that groups are timezone-compatible
        """

        def find_meta(tree, tzstatus, path=()):
            op = tree[0]
            path = path + (op,)
            metas = GMETAS.get(op)
            if metas:
                for name, metadata in metas(cn, self, tree).items():
                    tzaware = metadata['tzaware'] if metadata else None
                    # no naive operator (yet) so we don't
                    # have to handle it
                    tzstatus[(name, path)] = tzaware
            for item in tree:
                if isinstance(item, list):
                    find_meta(item, tzstatus, path)

        metamap = {}
        find_meta(tree, metamap)
        if not metamap:
            return {}

        def tzlabel(status):
            if status is None: return 'unknown'
            return 'tzaware' if status else 'tznaive'

        first_tzaware = next(iter(metamap.values()))
        for (name, path), tzaware in metamap.items():
            if first_tzaware != tzaware:
                raise ValueError(
                    f'Formula `{name}` has tzaware vs tznaive series:'
                    f'{",".join("`%s:%s`" % (k, tzlabel(v)) for k, v in metamap.items())}'
                )
        return first_tzaware

    @tx
    def register_group_formula(self, cn,
                               name, formula):
        if self.group_exists(cn, name) and self.group_type(cn, name) != 'formula':
            raise TypeError(
                f'cannot register formula `{name}`: already a `{self.group_type(cn, name)}`'
            )
        # basic syntax check
        tree = parse(formula)
        formula = serialize(tree)

        # type checking
        i = interpreter.GroupInterpreter(cn, self, {})
        rtype = helper.typecheck(tree, env=i.env)
        if not helper.sametype(rtype, pd.DataFrame):
            raise TypeError(
                f'formula `{name}` must return a `DataFrame`, not `{rtype.__name__}`'
            )

        # build metadata & check compat
        seriesmeta = self.find_metas(cn, tree)
        if not all(seriesmeta.values()) and reject_unknown:
            badseries = [k for k, v in seriesmeta.items() if not v]
            raise ValueError(
                f'Formula `{name}` refers to unknown series '
                f'{", ".join("`%s`" % s for s in badseries)}'
            )

        sql = (
            f'insert into "{self.namespace}".group_formula (name, text) '
            'values (%(name)s, %(text)s) '
            'on conflict (name) do update '
            'set text = %(text)s'
        )
        cn.execute(
            sql,
            name=name,
            text=formula
        )

        tzaware = self.check_group_tz_compatibility(cn, tree)
        coremeta = self.default_meta(tzaware)
        meta = self.group_metadata(cn, name) or {}
        meta = dict(meta, **coremeta)
        self.update_group_metadata(cn, name, meta, internal=True)

    @tx
    def group_formula(self, cn, groupname):
        res = cn.execute(
            f'select text from "{self.namespace}".group_formula '
            'where name = %(name)s',
            name=groupname
        )
        return res.scalar()

    @tx
    def list_groups(self, cn):
        cat = super().list_groups(cn)
        cat.update({
            name: 'formula'
            for name, in cn.execute(
                    f'select name from "{self.namespace}".group_formula'
            ).fetchall()
        })
        cat.update({
            name: 'bound'
            for name, in cn.execute(
                    f'select groupname from "{self.namespace}".group_binding'
            ).fetchall()
        })
        return cat

    @tx
    def group_delete(self, cn, name):
        kind = self.group_type(cn, name)
        if kind == 'primary':
            return super().group_delete(cn, name)

        if kind == 'formula':
            sql = (
                f'delete from "{self.namespace}".group_formula '
                'where name = %(name)s'
            )
        else:
            assert kind == 'bound'
            sql = (
                f'delete from "{self.namespace}".group_binding '
                'where groupname = %(name)s'
            )
        cn.execute(sql, name=name)

    @tx
    def group_get(self, cn, groupname,
                  revision_date=None,
                  from_value_date=None,
                  to_value_date=None):
        # case of formula
        formula = self.group_formula(cn, groupname)
        if formula:
            interp = interpreter.GroupInterpreter(
                cn, self,
                dict(
                    revision_date=revision_date,
                    from_value_date=from_value_date,
                    to_value_date=to_value_date
                )
            )
            df = self.eval_formula(
                cn, formula,
                revision_date=revision_date,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                __interpreter__=interp
            )
            if df.index.dtype != 'object':
                df.columns = [str(col) for col in df.columns]
            if df.index.name:
                df.index.name = None
            return df

        bindinfo = self.bindings_for(
            cn, groupname
        )
        if bindinfo:
            return self._hijacked_formula(
                cn,
                bindinfo[0],
                bindinfo[1],
                revision_date=revision_date,
                from_value_date=from_value_date,
                to_value_date=to_value_date
            )

        return super().group_get(
            cn,
            groupname,
            revision_date=revision_date,
            from_value_date=from_value_date,
            to_value_date=to_value_date
        )

    def find_groups_and_series(self, cn, tree):
        op = tree[0]
        super_finder = dict(GFINDERS, **FINDERS)
        finder = super_finder.get(op)
        seriestree = finder(cn, self, tree) if finder else {}
        for item in tree:
            if isinstance(item, list):
                seriestree.update(
                    self.find_groups_and_series(cn, item)
                )
        return seriestree

    @tx
    def group_insertion_dates(self, cn, name, **bounds):
        if not self.group_exists(cn, name):
            return None
        formula = self.group_formula(cn, name)

        if formula:
            tree = parse(formula)
            groups_and_series = self.find_groups_and_series(cn, tree)
            allrevs = []
            for name, info in groups_and_series.items():
                operator_name = str(info[0])
                if operator_name == 'group':
                    if not self.group_exists(cn, name):
                        continue
                    allrevs += self.group_insertion_dates(cn, name, **bounds)
                elif operator_name == 'series':
                    if not self.exists(cn, name):
                        continue
                    allrevs += self.insertion_dates(cn, name, **bounds)
                elif operator_name in GAUTO:
                    idates_func = GIDATES[operator_name]
                    allrevs += idates_func(cn, self, tree, **bounds)

            return sorted(set(allrevs))

        bindinfo = self.bindings_for(
            cn, name
        )

        if bindinfo:
            # hijacked formula
            # we only return the idates when all the groups are present
            # This API point merely provide the definition period
            # where the hijacking is fully usable (and backtestable)
            seriesname, bindings = bindinfo

            tree = helper.expanded(
                self,
                cn,
                parse(self.formula(cn, seriesname)),
                shownames=bindings['series'].values,
                scopes=False
            )
            series = self.find_series(cn, tree)

            # let's only keep the groups that are really used
            # users like to provide mappings that contain
            # other (irrelevant) groups
            groups_names = {
                gname for sname, gname in
                zip(
                    bindings['series'],
                    bindings['group']
                )
                if sname in series
            }

            all_idates = [
                self.group_insertion_dates(cn, gn, **bounds)
                for gn in groups_names
            ]
            first_date_with_all_presents = max(
                min(dates)
                for dates in all_idates
            )
            all_idates_bounded = [
                {
                    idate
                    for idate in idates
                    if idate >= first_date_with_all_presents
                 }
                for idates in all_idates
            ]
            final_dates = set()
            for dates in all_idates_bounded:
                final_dates = final_dates.union(dates)
            return sorted(final_dates)

        # primaries
        return super().group_insertion_dates(cn, name, **bounds)


    @tx
    def group_history(self, cn, name,
                      from_value_date=None,
                      to_value_date=None,
                      from_insertion_date=None,
                      to_insertion_date=None):
        idates = self.group_insertion_dates(
            cn,
            name,
            from_insertion_date=from_insertion_date,
            to_insertion_date=to_insertion_date
        )
        if idates is None:
            return None
        if not len(idates):
            return {}
        history = {}
        for idate in idates:
            history[idate] = self.group_get(
                cn,
                name,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                revision_date=idate
            )
        return history

    # group formula binding

    @tx
    def register_formula_bindings(self, cn, groupname, formulaname, binding, nockeck=False):
        if set(binding.columns) != {'series', 'group', 'family'}:
            raise ValueError(
                'bindings must have `series` `groups` and `family` columns'
            )

        if self.type(cn, formulaname) != 'formula':
            raise ValueError(f'`{formulaname}` is not a formula')

        if not nockeck:
            grtzstate = []
            for gname in binding.group:
                assert self.group_exists(cn, gname), f'Group `{gname}` does not exist.'
                grtzstate.append(
                    (gname, self.group_metadata(cn, gname)['tzaware'])
                )
            tstzstate = []
            for sname in binding.series:
                assert self.exists(cn, sname), f'Series `{sname}` does not exist.'
                tstzstate.append(
                    (sname, self.internal_metadata(cn, sname)['tzaware'])
                )
            for (gname, gtz), (sname, stz) in zip(grtzstate, tstzstate):
                assert gtz == stz, f'Series `{sname}` and group `{gname}` must be tz-compatible.'

        if not len(binding):
            raise ValueError(f'formula `{formulaname}` has an empty binding')

        gtype = self.group_type(cn, groupname)
        if self.group_exists(cn, groupname) and gtype != 'bound':
            raise ValueError(f'cannot bind `{groupname}`: already a {gtype}')

        cn.execute(
            f'insert into "{self.namespace}"."group_binding" (groupname, seriesname, binding) '
            'values (%(gname)s, %(sname)s, %(binding)s) '
            'on conflict (groupname) do update '
            'set seriesname = %(sname)s, '
            '    binding = %(binding)s',
            gname=groupname,
            sname=formulaname,
            binding=binding.to_json(orient='records')
        )

        seriesmeta = {
            k: v
            for k, v in self.metadata(cn, formulaname).items()
            if k in self.metakeys
        }
        metadata = self.group_metadata(cn, groupname) or {}
        # make sure we derive the internals from the series meta
        metadata.update(seriesmeta)

        self.update_group_metadata(cn, groupname, metadata, internal=True)

    @tx
    def bindings_for(self, cn, groupname):
        res = cn.execute(
            'select seriesname, binding '
            f'from "{self.namespace}".group_binding '
            'where groupname = %(gname)s',
            gname=groupname
        )
        sname_binding = res.fetchone()
        if sname_binding is None:
            return
        binding = pd.DataFrame(sname_binding[1])
        return sname_binding[0], binding

    @tx
    def get_bound_group(self, cn, name, binding,
                        from_value_date=None,
                        to_value_date=None,
                        revision_date=None):
        m = binding['series'] == name
        if sum(m) == 0:
            # unbound series
            return None, None

        assert sum(m) == 1
        groupname = binding.loc[m, 'group'].iloc[0]
        family = binding.loc[m, 'family'].iloc[0]
        return (
            self.group_get(
                cn,
                groupname,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                revision_date=revision_date,
            ),
            family
        )

    @tx
    def _hijacked_formula(self, cn, name, binding,
                          from_value_date=None,
                          to_value_date=None,
                          revision_date=None):
        # find all the series in the formula that are referenced in
        # the binding since the series can be anywhere in the
        # dependencies - we use exanded_formula using (shownames) to
        # limit the expansion to the minimum necessary to show such names.
        # This semi-expanded formula will be
        # used later in the interpretor
        tree = helper.expanded(
            self,
            cn,
            parse(self.formula(cn, name)),
            shownames=binding['series'].values,
            scopes=True,
        )
        new_tree = helper.inject_toplevel_bindings(
            tree,
            {
                'revision_date': revision_date,
                'from_value_date': from_value_date,
                'to_value_date': to_value_date,
            }
        )
        series = self.find_series(cn, new_tree)
        formula = serialize(new_tree)

        groupmap = defaultdict(dict)
        for sname in series:
            df, family = self.get_bound_group(
                cn,
                sname,
                binding,
                from_value_date,
                to_value_date,
                revision_date
            )
            if family is not None:
                assert df is not None
                groupmap[family][sname] = df

        assert groupmap

        bi = interpreter.BridgeInterpreter(
            cn, self, {},
            groups=groupmap,
            binding=binding,
        )

        # build scenarios combinations
        possible_values = []
        families = []
        for ens_name, sub in groupmap.items():
            # we may have several groups there
            # but they all have the same column names
            # and this is what we want
            df = sub[list(sub.keys())[0]]
            possible_values.append(df.columns.to_list())
            families.append(ens_name)

        # combination is a list of dict tha contain a unqiue scenario...
        # [{ens0 = 'scenario0', ens1 = 'scenario4'}, ... ]
        combinations = []
        for comb in itertools.product(*possible_values):
            combination = {}
            for idx, values in enumerate(comb):
                combination.update({families[idx]: values})
            combinations.append(combination)

        columns = []
        for combination in combinations:
            columns.append(bi.g_evaluate(formula, combination))

        return pd.concat(columns, axis=1)
