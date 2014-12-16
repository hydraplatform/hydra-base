try: #pragma: no cover
    from .tw2widgets.widgets import *
except ImportError: #pragma: no cover
    from .tw1widgets.widgets import *