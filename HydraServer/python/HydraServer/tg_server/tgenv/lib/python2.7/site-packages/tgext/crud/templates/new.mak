<%inherit file="local:templates.master"/>
<%namespace name="menu_items" file="tgext.crud.templates.menu_items"/>

<%def name="title()">
${tmpl_context.title} - New ${model}
</%def>
<%def name="meta()">
  ${menu_items.menu_style()}
  ${parent.meta()}
</%def>

<%def name="body_class()">tundra</%def>

<div id="main_content">
  ${menu_items.menu_items()}
  <div id="crud_content">
    <div class="crud_add">
      <h2>New ${model}</h2>
       ${tmpl_context.widget(value=value, action='./') | n}
    </div>
  </div>
  <div style="clear:both;"> &nbsp; </div>
</div>
