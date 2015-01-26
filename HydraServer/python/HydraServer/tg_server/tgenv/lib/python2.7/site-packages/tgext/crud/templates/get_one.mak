<%inherit file="local:templates.master"/>
<%namespace name="menu_items" file="tgext.crud.templates.menu_items"/>

<%def name="title()">
${tmpl_context.title} - View ${model}
</%def>

<%def name="body_class()">tundra</%def>
<%def name="meta()">
  ${menu_items.menu_style()}
  ${parent.meta()}
</%def>
<div>
<h1>${model} Listing</h1>
</div>
<div>
<a href='new/'>New</a> ${model}
</div>
<br/>
${tmpl_context.widget() | n}
</div>
</body>
</html>
