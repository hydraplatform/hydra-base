<%inherit file="local:templates.master"/>
<%namespace name="menu_items" file="tgext.crud.templates.menu_items"/>

<%def name="title()">
${tmpl_context.title} - ${model} Listing
</%def>
<%def name="meta()">
${menu_items.menu_style()}
${parent.meta()}
</%def>
<%def name="body_class()">tundra</%def>
<div id="main_content">
  ${menu_items.menu_items()}
  <div id="crud_content">
    <h1>${model} Listing</h1>
    <div id="crud_btn_new">
      <a href='${tg.url("new", params=tmpl_context.kept_params)}' class="add_link">New ${model}</a>
         % if tmpl_context.paginators:
           <span>${tmpl_context.paginators.value_list.pager(link=mount_point+'/')}</span>
         % endif
      % if search_fields:
        <div id="crud_search">
          <form>
              <select id="crud_search_field" onchange="crud_search_field_changed(this);">
                  % for field, name, selected in search_fields:
                    % if selected is not False:
                      <option value="${field}" selected="selected">${name}</option>
                    % else:
                      <option value="${field}">${name}</option>
                    % endif
                  % endfor
              </select>
              <input id="crud_search_value" name="${current_search[0]}" type="text" placeholder="equals / contains" value="${current_search[1]}" />
              <input type="submit" value="Search"/>
          </form>
        </div>
      % endif
    </div>
    <div class="crud_table">
     ${tmpl_context.widget(value=value_list, action=mount_point+'.json', attrs=dict(style="height:200px; border:solid black 3px;")) |n}
    </div>
  </div>
  <div style="clear:both;"> &nbsp; </div>
</div>
