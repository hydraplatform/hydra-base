# -*- mode: python -*-
a = Analysis(['server.py'],
             pathex=['F:\\work\\HYDRA\\svn\\HYDRA\\HydraServer\\trunk'],
             hiddenimports=['spyne.service', 'sqlalchemy', 'cherrypy', 'cherrypy.wsgiserver', 'zope.sqlalchemy', 'zope.interface', 'numpy', 'numpy.linalg', 'winpaths'],
             hookspath=None,
             runtime_hooks=None)

a.datas += [('HydraLib/static/unit_definitions.xml',
            '../../HydraLib/trunk/HydraLib/static/unit_definitions.xml', 'DATA'), 
            ('HydraLib/static/user_units.xml',
            '../../HydraLib/trunk/HydraLib/static/user_units.xml', 'DATA'),
            ]

pyz = PYZ(a.pure)
exe = EXE(pyz,
          a.scripts,
          exclude_binaries=True,
          name='HydraServer.exe',
          debug=False,
          strip=None,
          upx=True,
          console=True )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=None,
               upx=True,
               name='HydraServer')
