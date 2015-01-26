from .utils import sprox_with_tw2

if sprox_with_tw2():
    from tw2.core import CSSSource, JSSource
else:
    from tw.api import CSSSource, JSSource


crud_script = JSSource(location='headbottom',
                       src='''
    function crud_search_field_changed(select) {
        var selected = '';
        for (var idx=0; idx != select.options.length; ++idx) {
            if (select.options[idx].selected)
                selected = select.options[idx];
        }
        var field = document.getElementById('crud_search_value');
        field.name = selected.value;
    }
''')


crud_style = CSSSource(location='headbottom',
                       src='''
    #menu_items {
      padding:0px 12px 0px 2px;
      list-style-type:None;
      padding-left:0px;
    }

    #crud_leftbar {
        float:left;
        padding-left:0px;
    }

    #crud_content {
        float:left;
        width:80%;
    }

    #crud_content > h1,
    .crud_edit > h2,
    .crud_add > h2 {
        margin-top: 1px;
    }

    #crud_btn_new {
        margin:1ex 0;
    }

    #crud_btn_new > span {
        margin-left:2em;
    }

    #crud_search {
        float: right;
    }

    #crud_search input {
        border: 1px solid #CCC;
        background-color: white;
    }

    #crud_search input:hover {
        background-color: #EFEFEF;
    }
''')