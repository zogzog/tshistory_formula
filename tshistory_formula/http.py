import pandas as pd
import requests

from flask_restx import (
    inputs,
    Resource,
    reqparse
)

from tshistory.http.client import Client, unwraperror
from tshistory.http.util import onerror
from tshistory.http.server import httpapi


base = reqparse.RequestParser()
base.add_argument(
    'name',
    type=str,
    required=True,
    help='timeseries name'
)

formula = base.copy()
formula.add_argument(
    'expanded',
    type=inputs.boolean,
    default=False,
    help='return the recursively expanded formula'
)

formula_components = base.copy()
formula_components.add_argument(
    'expanded',
    type=inputs.boolean,
    default=False,
    help='return the recursively expanded formula components'
)

register_formula = base.copy()
register_formula.add_argument(
    'text',
    type=str,
    required=True,
    help='source of the formula'
)
register_formula.add_argument(
    'reject_unknown',
    type=inputs.boolean,
    default=True,
    help='fail if the referenced series do not exist'
)
register_formula.add_argument(
    # note: `update` won't work as it is a method of parse objects
    'force_update',
    type=inputs.boolean,
    default=False,
    help='accept to update an existing formula if true'
)

# groups

groupbase = reqparse.RequestParser()
groupbase.add_argument(
    'name',
    type=str,
    required=True,
    help='group name'
)

register_group_formula = groupbase.copy()
register_group_formula.add_argument(
    'text',
    type=str,
    required=True,
    help='source of the formula'
)

groupformula = groupbase.copy()
groupformula.add_argument(
    'expanded',
    type=inputs.boolean,
    default=False,
    help='return the recursively expanded formula'
)

boundformula = groupbase.copy()
boundformula.add_argument(
    'formulaname',
    type=str,
    help='name of the formula to exploit (create/update)'
)
boundformula.add_argument(
    'bindings',
    type=str,
    help='json representation of the bindings (create/update)'
)


class formula_httpapi(httpapi):

    def routes(self):
        super().routes()

        tsa = self.tsa
        api = self.api
        nss = self.nss
        nsg = self.nsg

        @nss.route('/formula')
        class timeseries_formula(Resource):

            @api.expect(formula)
            @onerror
            def get(self):
                args = formula.parse_args()
                if not tsa.exists(args.name):
                    api.abort(404, f'`{args.name}` does not exists')

                if not tsa.type(args.name):
                    api.abort(409, f'`{args.name}` exists but is not a formula')

                form = tsa.formula(args.name, args.expanded)
                return form, 200

            @api.expect(register_formula)
            @onerror
            def patch(self):
                args = register_formula.parse_args()

                exists = tsa.formula(args.name)
                try:
                    tsa.register_formula(
                        args.name,
                        args.text,
                        reject_unknown=args.reject_unknown,
                        update=args.force_update
                    )
                except TypeError as err:
                    api.abort(409, err.args[0])
                except ValueError as err:
                    api.abort(409, err.args[0])
                except AssertionError as err:
                    api.abort(409, err.args[0])
                except SyntaxError:
                    api.abort(400, f'`{args.name}` has a syntax error in it')
                except Exception:
                    raise

                return '', 200 if exists else 201

        @nss.route('/formula_components')
        class timeseries_formula_components(Resource):

            @api.expect(formula_components)
            def get(self):
                args = formula_components.parse_args()

                if not tsa.exists(args.name):
                    api.abort(404, f'`{args.name}` does not exists')

                if not tsa.type(args.name):
                    api.abort(409, f'`{args.name}` exists but is not a formula')

                form = tsa.formula_components(args.name, args.expanded)
                return form, 200

        @nsg.route('/formula')
        class group_formula(Resource):

            @api.expect(groupformula)
            @onerror
            def get(self):
                args = groupformula.parse_args()
                if not tsa.group_exists(args.name):
                    api.abort(404, f'`{args.name}` does not exists')

                if not tsa.group_type(args.name):
                    api.abort(409, f'`{args.name}` exists but is not a formula')

                form = tsa.group_formula(args.name, args.expanded)
                return form, 200

            @api.expect(register_group_formula)
            @onerror
            def put(self):
                args = register_group_formula.parse_args()

                exists = tsa.group_formula(args.name)
                try:
                    tsa.register_group_formula(
                        args.name,
                        args.text
                    )
                except TypeError as err:
                    api.abort(409, err.args[0])
                except ValueError as err:
                    api.abort(409, err.args[0])
                except AssertionError as err:
                    api.abort(409, err.args[0])
                except SyntaxError:
                    api.abort(400, f'`{args.name}` has a syntax error in it')
                except Exception:
                    raise

                return '', 200 if exists else 201

            @nsg.route('/boundformula')
            class bound_formula(Resource):

                @api.expect(boundformula)
                @onerror
                def get(self):
                    args = boundformula.parse_args()
                    if not tsa.group_exists(args.name):
                        api.abort(404, f'`{args.name}` does not exists')

                    if tsa.group_type(args.name) != 'bound':
                        api.abort(409, f'`{args.name}` exists but is not a bound formula')

                    name, bindings = tsa.bindings_for(args.name)
                    return (name, bindings.to_dict(orient='records')), 200

                @api.expect(boundformula)
                @onerror
                def put(self):
                    args = boundformula.parse_args()
                    bindings = pd.read_json(args.bindings)
                    tsa.register_formula_bindings(
                        args.name,
                        args.formulaname,
                        bindings
                    )

                    return '', 200


class FormulaClient(Client):

    @unwraperror
    def formula(self, name, expanded=False):
        res = requests.get(
            f'{self.uri}/series/formula', params={
                'name': name,
                'expanded': expanded
            }
        )
        if res.status_code == 200:
            return res.json()
        if res.status_code == 418:
            return res
        return  # None is the reasonable api answer

    @unwraperror
    def formula_components(self, name, expanded=False):
        res = requests.get(
            f'{self.uri}/series/formula_components', params={
                'name': name,
                'expanded': expanded
            }
        )
        if res.status_code == 200:
            return res.json()
        if res.status_code == 418:
            return res
        return  # None is the reasonable api answer

    @unwraperror
    def register_formula(self, name,
                         formula,
                         reject_unknown=True,
                         update=False):
        res = requests.patch(
            f'{self.uri}/series/formula', data={
                'name': name,
                'text': formula,
                'reject_unknown': reject_unknown,
                'force_update': update
            }
        )
        if res.status_code == 400:
            raise SyntaxError(res.json()['message'])
        elif res.status_code == 409:
            msg = res.json()['message']
            if 'unknown' in msg:
                raise ValueError(msg)
            elif 'exists' in msg:
                raise AssertionError(msg)

        if res.status_code in (200, 204):
            return

        return res

    @unwraperror
    def group_formula(self, name, expanded=False):
        res = requests.get(
            f'{self.uri}/group/formula', params={
                'name': name,
                'expanded': expanded
            }
        )
        if res.status_code == 200:
            return res.json()
        if res.status_code == 418:
            return res
        if res.status_code == 404:
            return
        return res

    @unwraperror
    def register_group_formula(self, name, formula):
        res = requests.put(
            f'{self.uri}/group/formula', data={
                'name': name,
                'text': formula
            }
        )
        if res.status_code == 400:
            raise SyntaxError(res.json()['message'])
        elif res.status_code == 409:
            msg = res.json()['message']
            if 'unknown' in msg:
                raise ValueError(msg)
            elif 'exists' in msg:
                raise AssertionError(msg)
            else:
                raise TypeError(msg)

        if res.status_code in (200, 204):
            return

        return res

    @unwraperror
    def register_formula_bindings(self, name, formulaname, bindings):
        res = requests.put(
            f'{self.uri}/group/boundformula', data={
                'name': name,
                'formulaname': formulaname,
                'bindings': bindings.to_json(orient='records')
            }
        )
        return res

    @unwraperror
    def bindings_for(self, name):
        res = requests.get(
            f'{self.uri}/group/boundformula', params={
                'name': name
            }
        )
        if res.status_code == 200:
            name, bindings = res.json()
            return name, pd.DataFrame(bindings)

        if res.status_code == 404:
            return None

        return res
