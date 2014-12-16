<%inherit file="local:templates.master"/>

<%def name="title()">
${tmpl_context.title} - ${model} Listing
</%def>

<%
PAGER_ARGS = tmpl_context.make_pager_args(link=mount_point+'/',
                                          page_link_template='<li><a%s>%s</a></li>',
                                          page_plain_template='<li%s><span>%s</span></li>',
                                          curpage_attr={'class': 'active'})
%>

<div class="row">
    <div class="col-md-2">
      % if hasattr(tmpl_context, 'menu_items'):
        <ul class="nav crud-sidebar hidden-xs hidden-sm">
          % for lower, item in sorted(tmpl_context.menu_items.items()):
            <li class="${item==model and 'active' or ''}">
                <a href="${tmpl_context.crud_helpers.make_link(lower)}">${item}</a>
            </li>
          % endfor
        </ul>
      % endif
    </div>

    <div class="col-md-10">
      <h1 class="page-header">${model} Listing</h1>

      <div class="row">
        <div class="col-xs-3 col-md-2">
          <a class="btn btn-success"
             href='${tg.url("new", params=tmpl_context.kept_params)}'>New ${model}</a>
        </div>

        <div class="col-xs-9 col-md-3">
         % if tmpl_context.paginators:
          <ul class="pagination pull-sm-right" style="margin:0;">
            ${tmpl_context.paginators.value_list.pager(**PAGER_ARGS)}
          </ul>
         % endif
        </div>

        <div class="col-xs-12 col-md-7">
          <div class="hidden-lg hidden-md">&nbsp;</div>
          % if search_fields:
            <form class="form-inline pull-md-right">
              <div class="form-group">
                <select id="crud_search_field" class="form-control"
                        onchange="crud_search_field_changed(this);">
                  % for field, name, selected in search_fields:
                    % if selected is not False:
                      <option value="${field}" selected="selected">${name}</option>
                    % else:
                      <option value="${field}">${name}</option>
                    % endif
                  % endfor
                </select>
              </div>

              <div class="form-group">
                <input id="crud_search_value" class="form-control" type="text"
                       placeholder="equals / contains"
                       name="${current_search[0]}" value="${current_search[1]}"/>
              </div>

              <button type="submit" class="btn btn-default">Search</button>
            </form>
          % endif
        </div>
      </div>


      <br/>

      <div>
        ${tmpl_context.widget(value=value_list, action=mount_point+'.json')|n}
      </div>
    </div>
</div>