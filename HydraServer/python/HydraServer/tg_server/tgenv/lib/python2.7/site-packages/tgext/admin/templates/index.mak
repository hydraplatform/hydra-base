<%inherit file="local:templates.master"/>

<%def name="title()">
Turbogears Administration System
</%def>

<div style="height:0px;"> &nbsp; </div>
    <h2>TurboGears Admin</h2>
    This is a fully-configurable administrative tool to help you administer your website.
    Below is links to all of your models.<br/>    They will bring you to a listing of the objects
    in your database.

<table class="admin_grid">
  % for model in models:
    <tr py:for="model in models">
      <td>
        <a href='${model.lower()}s/' class="edit_link">${model}</a>
      </td>
    </tr>
  % endfor
</table>
