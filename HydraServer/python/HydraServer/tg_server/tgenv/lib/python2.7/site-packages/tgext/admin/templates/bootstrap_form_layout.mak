<%namespace name="tw" module="tw2.core.mako_util"/>\
<div ${tw.attrs(attrs=w.attrs)}>
    % for c in w.children_hidden:
        ${c.display() | n}
    % endfor

    % for c in w.children_non_hidden:
    <div class="${((c.validator and getattr(c.validator, 'required', getattr(c.validator, 'not_empty', False))) and ' required' or '') + (c.error_msg and ' has-error' or '')}"\
      % if w.hover_help and c.help_text:
        title="${c.help_text}" \
      % endif
    >
      <div ${tw.attrs(attrs=c.container_attrs)}>
        <label for="${c.compound_id}" ${tw.attrs(attrs=getattr(c, 'label_attrs', {}))}>${ c.label or '' }</label>
        <div ${tw.attrs(attrs=getattr(c, 'wrapper_attrs', {}))}>${c.display() | n}</div>
        % if not w.hover_help:
            ${c.help_text or ''}
        % endif
        <span id="${c.compound_id or ''}:error" class="error help-block">${c.error_msg or ''}</span>
      </div>
    </div>
    % endfor

   <div class="error"><span id="${w.compound_id or ''}:error" class="error">\
        %for error in w.rollup_errors:
            <p>${error}</p>
        %endfor
    </span></div>
</div>