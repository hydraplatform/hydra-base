"""Dojo Specific Widgets"""

from tw.dojo import DojoQueryReadStore, DojoBase, grid_css, tundragrid_css, DojoJsonRestStore, buildService
from tw.dojo.selectshuttle import DojoSelectShuttleField, DojoSortedSelectShuttleField
from tw.api import JSSource
from sprox.widgets import PropertyMixin

from tw.dojo import DojoBase, tundragrid_css, tundra_css, dojo_css, dojo_js
from tw.core import JSLink

sprox_grid_js = JSLink(modname="sprox",
                       filename="widgets/tw1widgets/static/dojo_grid.js",
                       )

class SproxDojoGrid(DojoBase):
    engine_name=None
    available_engines = ['mako', 'genshi']
    css = [grid_css, tundragrid_css, tundra_css]
    javascript=[dojo_js, sprox_grid_js]
    dojoType = 'dojox.grid.DataGrid'
    require = ['dojox.grid.DataGrid', 'twdojo.data.TWDojoRestStore']
    params = ['id', 'attrs', 'columns', 'jsId', 'action',
              'rowsPerPage', 'model', 'delayScroll', 'cssclass', 'actions',
              'columnResizing', 'columnReordering', 'column_widths', 
              'default_column_width', 'headers','column_options', 
              'default_column_options','dojoStoreType','dojoStoreWidget',
              'autoHeight'
              ]
    autoHeight="false"
    delayScroll = "true"
    cssclass="sprox-dojo-grid"
    rowsPerPage = 20
    columns = []
    columnReordering = "false"
    columnResizing="false"
    column_widths = {}
    column_options = {}
    default_column_options = {}
    headers = {}
    default_column_width = "30px"
    include_dynamic_js_calls = True
    action='.json'
    model = None
    actions = True
    dojoStoreType = 'dojox.data.QueryReadStore'
    dojoStoreWidget = None
    template = "sprox.widgets.tw1widgets.templates.dojogrid"
    #attrs = {'style':"height:200px"}
    def update_params(self,d):
        d['dojoStoreWidget']=DojoQueryReadStore()
        super(SproxDojoGrid, self).update_params(d)

class DojoJsonRestStoreInstance(JSSource):
    engine_name='mako'
    require = ['twdojo.data.TWDojoRestStore']
    target='.json'
    url = '.json'
    idAttribute = 'id'
    autoSave = 'true'
    source_vars = ["varId","target","url","idAttribute","autoSave"]
    src = """
    dojo.require("twdojo.data.TWDojoRestStore")
    var ${varId}=new twdojo.data.TWDojoRestStore({target:"${target}",autoSave:"${autoSave and 'true' or 'false'}", service:buildService("${url}"),idAttribute:"${idAttribute}"})
                """

class SproxEditableDojoGrid(DojoBase):
    engine_name=None
    available_engines = ['genshi', 'mako']
    css = [grid_css, tundragrid_css]
    require = ['dojox.grid.DataGrid', 'twdojo.data.TWDojoRestStore']
    dojoType = 'dojox.grid.DataGrid'
    params = ['id', 'attrs', 'columns', 'jsId', 'action',
              'rowsPerPage', 'model', 'delayScroll', 'cssclass', 'actions',
              'columnResizing', 'columnReordering', 'column_widths', 'default_column_width', 'headers','column_options', 'default_column_options','dojoStoreType','dojoStoreWidget',
              'autoHeight'
              ]
    delayScroll = "true"
    cssclass="sprox-dojo-grid"
    rowsPerPage = 20
    columns = []
    columnReordering = "false"
    columnResizing="false"
    column_widths = {}
    column_options = {}
    default_column_options = {'editable':"true"}
    headers = {}
    default_column_width = "10em"
    include_dynamic_js_calls = True
    action='.json'
    model = None
    actions = True
    dojoStoreType = 'twdojo.data.TWDojoRestStore'
    dojoStoreWidget = None
    template = "sprox.widgets.tw1widgets.templates.dojogrid"

    def __init__(self,**kw):
        super(SproxEditableDojoGrid, self).__init__(**kw)
        storeName = kw.get('jsId','') or ''
        storeName = storeName+'_store'

        self.javascript.append(buildService)
        self.javascript.append(DojoJsonRestStoreInstance(varId=storeName,target=kw['action'],url=kw['action']))


class SproxDojoSelectShuttleField(DojoSelectShuttleField, PropertyMixin):
    def update_params(self, d):
        self._my_update_params(d)
        super(SproxDojoSelectShuttleField, self).update_params(d)

class SproxDojoSortedSelectShuttleField(DojoSortedSelectShuttleField, PropertyMixin):
    def update_params(self, d):
        self._my_update_params(d)
        super(SproxDojoSortedSelectShuttleField, self).update_params(d)

