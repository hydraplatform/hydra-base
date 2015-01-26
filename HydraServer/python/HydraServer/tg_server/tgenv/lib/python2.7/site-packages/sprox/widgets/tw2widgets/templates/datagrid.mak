<div>
<table class="${w.css_class}">
    % if w.columns:
    <thead>
        <tr>
            % for i, col in enumerate(w.columns):
                <th  class="col_${str(i)}">${col.title}</th>
            % endfor
        </tr>
    </thead>
    % endif

    <tbody>
    % if w.value:
        % for i, row in enumerate(w.value):
            <tr class="${i%2 and 'odd' or 'even'}">
                % for j, col in enumerate(w.columns):
                <td class="col_${str(j)}">
                    % if col.title == 'actions' or col.name in w.xml_fields:
                        ${col.get_field(row, displays_on='mako') | n}
                    % else:
                        ${col.get_field(row, displays_on='mako')}
                    % endif
                </td>
                %endfor
            </tr>
        % endfor
    % endif
    </tbody>
</table>
    % if not w.value:
      No Records Found.
    % endif
</div>
