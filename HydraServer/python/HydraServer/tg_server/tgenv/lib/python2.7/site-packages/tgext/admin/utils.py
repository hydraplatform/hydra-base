PAGER_HAS_TEMPLATE = False

try:
    import tg.support.paginate
    if not (hasattr(tg.support.paginate, '_make_link') and
            hasattr(tg.support.paginate, '_make_span')):
        PAGER_HAS_TEMPLATE = True
except ImportError:
    pass


def make_pager_args(**kwargs):
    if not PAGER_HAS_TEMPLATE:
        kwargs.pop('page_link_template', None)
        kwargs.pop('page_plain_template', None)
    return kwargs