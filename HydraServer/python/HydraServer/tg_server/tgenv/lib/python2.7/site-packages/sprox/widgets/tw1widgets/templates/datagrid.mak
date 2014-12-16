<div>
<table id="${id}" class="${css_class}">
    <thead>
        <tr>
            % for i, col in enumerate(columns):
                <th  class="col_${i}">${col.title}</th>
            % endfor
        </tr>
    </thead>
    <tbody>
    % if value:
        % for i, row in enumerate(value):
            <tr class="${i%2 and 'odd' or 'even'}">
                % for j, col in enumerate(columns):
                <td class="col_${j}">
                    % if col.title == 'actions' or col.title in xml_fields:
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
    % if not value:
      No Records Found.
    % endif
</div>