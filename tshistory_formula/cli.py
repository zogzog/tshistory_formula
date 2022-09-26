import json
from pprint import pprint
import hashlib

import click
import pandas as pd
from sqlalchemy import create_engine
from psyl.lisp import (
    parse,
    serialize
)
from tshistory.util import find_dburi

from tshistory_formula.schema import formula_schema
from tshistory_formula.tsio import timeseries
from tshistory_formula.helper import (
    rename_operator,
    typecheck
)
from tshistory_formula.interpreter import Interpreter


@click.command(name='update-formula-metadata')
@click.argument('dburi')
@click.option('--reset', is_flag=True, default=False)
@click.option('--seriesname')
@click.option('--namespace', default='tsh')
def update_metadata(dburi, reset=False, seriesname=None, namespace='tsh'):
    engine = create_engine(find_dburi(dburi))
    tsh = timeseries(namespace)

    if seriesname:
        series = [seriesname]
    else:
        series = [
            name for name, kind in tsh.list_series(engine).items()
            if kind == 'formula'
        ]

    if reset:
        for name in series:
            # reset
            meta = tsh.metadata(engine, name)
            if meta:
                meta = {
                    k: v for k, v in meta.items()
                    if k not in tsh.metakeys
                }
            else:
                meta = {}
            sql = (f'update "{namespace}".formula '
                   'set metadata = %(metadata)s '
                   'where name = %(name)s')
            print('reset', name, 'to', meta)
            with engine.begin() as cn:
                cn.execute(
                    sql,
                    metadata=json.dumps(meta),
                    name=name
                )

    todo = []
    errors = []

    def justdoit():
        for name in series:
            tree = parse(tsh.formula(engine, name))
            try:
                tz = tsh.check_tz_compatibility(engine, tree)
                print(name, tz)
                meta = tsh.default_meta(tz)
            except ValueError as err:
                errors.append((name, err))
                continue
            if not meta or 'index_dtype' not in meta:
                todo.append(name)
                print(' -> todo')
                continue
            tsh.update_metadata(engine, name, meta)

    justdoit()

    print('TODO', todo)
    print('FAIL', len(errors))
    pprint(dict(errors))


@click.command(name='ingest-formulas')
@click.argument('dburi')
@click.argument('formula-file', type=click.Path(exists=True))
@click.option('--strict', is_flag=True, default=False)
@click.option('--namespace', default='tsh')
def ingest_formulas(dburi, formula_file, strict=False, namespace='tsh'):
    """ingest a csv file of formulas

    Must be a two-columns file with a header "name,text"
    """
    engine = create_engine(find_dburi(dburi))
    df = pd.read_csv(formula_file)
    tsh = timeseries(namespace)
    with engine.begin() as cn:
        for row in df.itertuples():
            print('ingesting', row.name)
            tsh.register_formula(
                cn,
                row.name,
                row.text,
                strict
            )


@click.command(name='typecheck-formula')
@click.argument('db-uri')
@click.option('--pdbshell', is_flag=True, default=False)
@click.option('--namespace', default='tsh')
def typecheck_formula(db_uri, pdbshell=False, namespace='tsh'):
    engine = create_engine(find_dburi(db_uri))
    tsh = timeseries(namespace)

    i = Interpreter(engine, tsh, {})
    for name, kind in tsh.list_series(engine).items():
        if kind != 'formula':
            continue

        formula = tsh.formula(engine, name)
        parsed = parse(formula)
        print(name, f'`{parsed[0]}`')
        typecheck(parsed, env=i.env)

    if pdbshell:
        import ipdb; ipdb.set_trace()


@click.command(name='test-formula')
@click.argument('db-uri')
@click.argument('formula')
@click.option('--pdbshell', is_flag=True, default=False)
@click.option('--namespace', default='tsh')
def test_formula(db_uri, formula, pdbshell=False, namespace='tsh'):
    engine = create_engine(find_dburi(db_uri))
    tsh = timeseries(namespace)

    ts = tsh.eval_formula(engine, formula)
    print(ts)
    if pdbshell:
        import ipdb; ipdb.set_trace()


@click.command(name='formula-init-db')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def init_db(db_uri, namespace):
    "initialize the formula part of a timeseries history schema"
    engine = create_engine(find_dburi(db_uri))
    formula_schema(namespace).create(engine)


@click.command(name='migrate-to-formula-groups')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def migrate_to_groups(db_uri, namespace='tsh'):
    engine = create_engine(find_dburi(db_uri))

    ns = namespace
    sql = f"""
    create table if not exists "{ns}".group_formula (
      id serial primary key,
      name text unique not null,
      text text not null,
      metadata jsonb
    );

    create table if not exists "{ns}".group_binding (
      id serial primary key,
      groupname text unique not null,
      seriesname text not null,
      binding jsonb not null,
      metadata jsonb
    );
    """

    with engine.begin() as cn:
        cn.execute(sql)


@click.command(name='migrate-to-content-hash')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def migrate_to_content_hash(db_uri, namespace='tsh'):
    from psyl import lisp
    engine = create_engine(find_dburi(db_uri))
    tsh = timeseries(namespace)

    chs = []
    series = engine.execute(
        f'select name, text from "{namespace}".formula'
    ).fetchall()
    print(f'Preparing {len(series)}.')

    for idx, (name, text) in enumerate(series):
        print(idx, name)
        ch = hashlib.sha1(
            lisp.serialize(
                tsh._expanded_formula(engine, text)
            ).encode()
        ).hexdigest()
        chs.append(
            {'name': name, 'contenthash': ch}
        )

    sql = (
        f'alter table "{namespace}".formula '
        'add column if not exists contenthash text not null default \'\';'
    )

    with engine.begin() as cn:
        cn.execute(sql)
        cn.execute(
            f'update "{namespace}".formula '
            f'set contenthash = %(contenthash)s '
            f'where name = %(name)s',
            chs
        )

        cn.execute(
            f'alter table "{namespace}".formula '
            f'alter column contenthash drop default;'
        )


@click.command(name='rename-operators')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def rename_operators(db_uri, namespace='tsh'):
    engine = create_engine(find_dburi(db_uri))
    tsh = timeseries(namespace)

    def rename(series):
        rewritten = []
        print(f'Transforming {len(series)} series.')
        for idx, (name, text) in enumerate(series):
            print(idx, name, text)
            tree0 = parse(text)
            tree1 = rename_operator(tree0, 'min', 'row-min')
            tree2 = rename_operator(tree1, 'max', 'row-max')
            tree3 = rename_operator(tree2, 'timedelta', 'shifted')
            tree4 = rename_operator(tree3, 'shift', 'time-shifted')
            rewritten.append(
                {'name': name, 'text': serialize(tree4)}
            )
        with engine.begin() as cn:
            cn.execute(
                f'update "{namespace}".formula '
                f'set text = %(text)s '
                f'where name = %(name)s',
                rewritten
            )

    series = engine.execute(
        f'select name, text from "{namespace}".formula'
    ).fetchall()

    if series:
        rename(series)

    series = engine.execute(
        f'select name, text from "{namespace}".group_formula'
    ).fetchall()

    if series:
        rename(series)


@click.command(name='migrate-to-dependants')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def migrate_to_dependants(db_uri, namespace='tsh'):
    engine = create_engine(find_dburi(db_uri))

    sql = """
create table if not exists "{ns}".dependant (
  sid int not null references "{ns}".formula(id) on delete cascade,
  needs int not null references "{ns}".formula(id) on delete cascade,
  unique(sid, needs)
);

create index if not exists "ix_{ns}_dependant_sid" on "{ns}".dependant (sid);
create index if not exists "ix_{ns}_dependant_needs" on "{ns}".dependant (needs);
""".format(ns=namespace)

    with engine.begin() as cn:
        cn.execute(sql)

    with engine.begin() as cn:
        # purge
        cn.execute(f'delete from "{namespace}".dependant')

    series = engine.execute(
        f'select name, text from "{namespace}".formula'
    ).fetchall()
    tsh = timeseries(namespace)

    for name, text in series:
        tsh.register_dependants(engine, name, parse(text))


@click.command(name='shell')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def shell(db_uri, namespace='tsh'):
    from tshistory.api import timeseries as tsapi
    tsa = tsapi(find_dburi(db_uri), namespace, timeseries)
    import pdb; pdb.set_trace()
