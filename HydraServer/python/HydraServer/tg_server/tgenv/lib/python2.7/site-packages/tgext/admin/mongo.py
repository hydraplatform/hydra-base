from .config import AdminConfig
from .tgadminconfig import UserControllerConfig, GroupControllerConfig, PermissionControllerConfig
from .layouts import BootstrapAdminLayout


class MongoAdminConfig(AdminConfig):
    pass


class TGMongoAdminConfig(MongoAdminConfig):
    user       = UserControllerConfig
    group      = GroupControllerConfig
    permission = PermissionControllerConfig 

    def __init__(self, models, translations=None):
        if not translations:
            translations =  {'group_id':'_id',
                              'user_id':'_id',
                        'permission_id':'_id'}

        super(MongoAdminConfig, self).__init__(models, translations)


class BootstrapTGMongoAdminConfig(TGMongoAdminConfig):
    layout = BootstrapAdminLayout