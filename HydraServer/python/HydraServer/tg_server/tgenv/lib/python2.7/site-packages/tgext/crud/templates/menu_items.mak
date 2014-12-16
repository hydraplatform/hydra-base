<%def name="menu_style()">
</%def>

<%def name="menu_items(pk_count=0)">
  <div id="crud_leftbar">
        <ul id="menu_items">
        % if hasattr(tmpl_context, 'menu_items'):
           % for lower, item in sorted(tmpl_context.menu_items.items()):
            <li>
                <a href="${tmpl_context.crud_helpers.make_link(lower, pk_count)}">${item}</a>
            </li>
           % endfor
        % endif
        </ul>
  </div>
</%def>