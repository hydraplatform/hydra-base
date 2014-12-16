<%namespace name="tw" module="tw.core.mako_util"/>\
<div> <!-- begin table widget mako! -->
<script>
//<![CDATA[
function lessThan(str) {
return str.replace(/&lt;/gi, "<");
}
//]]>
</script>
% if not(dojoStoreWidget is None):
<div dojoType="${dojoStoreType}" jsId="${jsId}_store"  id="${jsId}_store" url="${action}"></div>
% endif

<table  dojoType="${dojoType}"\
 jsId="${jsId}"\
 id="${id}"\
 store="${jsId}_store"\
 columnReordering="${columnReordering}"\
 rowsPerPage="${rowsPerPage}"\
 model="${model}"\
 delayScroll="${delayScroll}"\
 autoHeight="${autoHeight}"\
 class="${cssclass}"\
 escapeHTMLInData="${(attrs and (attrs.get('escapeHTMLInData', False) and 'True')) or 'False'}"\
 ${tw.attrs(attrs=attrs)}\
>
    <thead>
            <tr>
                % for column in columns:
                    <th formatter="lessThan" width="${column_widths.get(column, default_column_width)}" field="${column}" \
                         %for name,value in column_options.get(column,default_column_options).iteritems():
                            ${name}="${value}"\
                        %endfor
>${headers.get(column, column)}</th>
                % endfor
            </tr>
    </thead>
</table>
</div> <!-- end table widget -->