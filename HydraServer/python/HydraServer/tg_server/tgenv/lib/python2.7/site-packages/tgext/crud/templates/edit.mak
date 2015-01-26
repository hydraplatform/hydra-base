<%inherit file="local:templates.master"/>
<%namespace name="menu_items" file="tgext.crud.templates.menu_items"/>

<%def name="title()">
${tmpl_context.title} - ${model}
</%def>

<%def name="body_class()">tundra</%def>
<%def name="meta()">
  ${menu_items.menu_style()}
  ${parent.meta()}
</%def>
  <div id="main_content">
    ${menu_items.menu_items(pk_count)}
    <div id="crud_content">
      <div class="crud_edit">
        <h2>Edit ${model}</h2>
         ${tmpl_context.widget(value=value, action='./') | n}
      </div>
    </div>
  <div style="height:0px; clear:both;"> &nbsp; </div>
  </div> <!-- end main_content -->
