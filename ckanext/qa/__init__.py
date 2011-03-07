# this is a namespace package
try:
    import pkg_resources
    pkg_resources.declare_namespace(__name__)
except ImportError:
    import pkgutil
    __path__ = pkgutil.extend_path(__path__, __name__)

import os
from logging import getLogger

from genshi.input import HTML
from genshi.filters import Transformer

import ckan.lib.helpers as h

from ckan.plugins import implements, SingletonPlugin
from ckan.plugins import IRoutes, IConfigurer
from ckan.plugins import IConfigurable, IGenshiStreamFilter

import html

log = getLogger(__name__)

class QA(SingletonPlugin):
    
    implements(IConfigurable)
    implements(IGenshiStreamFilter)
    implements(IRoutes, inherit=True)
    implements(IConfigurer, inherit=True)
    
    def configure(self, config):
        self.enable_organizations = config.get('qa.organizations', False)

    def filter(self, stream):
        if self.enable_organizations:
            from pylons import request, tmpl_context as c
            routes = request.environ.get('pylons.routes_dict')
        
            data = dict(link = h.link_to("Organizations who have published packages with broken resource links.",\
                h.url_for(controller='qa',\
                action='organizations_with_broken_resource_links')
            ))

            if routes.get('controller') == 'ckanext.qa.controllers.view:ViewController'\
               and routes.get('action') == 'index':
                stream = stream | Transformer('body//div[@class="qa-content"]')\
                    .append(HTML(html.ORGANIZATION_LINK % data))
                        
        return stream
        
    def before_map(self, map):
        map.connect('qa', '/qa',
            controller='ckanext.qa.controllers.view:ViewController',
            action='index')
            
        map.connect('qa_action', '/qa/{action}',
            controller='ckanext.qa.controllers.view:ViewController')

        map.connect('qa_action', '/qa/{action}/:id',
            controller='ckanext.qa.controllers.view:ViewController')
                
        map.connect('qa_api', '/api/2/util/qa/{action}',
            conditions=dict(method=['GET']),
            controller='ckanext.qa.controllers.api:ApiController')
                
        map.connect('qa_api_resource_formatted',
                    '/api/2/util/qa/{action}/:(id).:(format)',
            conditions=dict(method=['GET']),
            controller='ckanext.qa.controllers.api:ApiController')
                
        map.connect('qa_api_resource', '/api/2/util/qa/{action}/:id',
            conditions=dict(method=['GET']),
            controller='ckanext.qa.controllers.api:ApiController')
                
        return map

    def update_config(self, config):
        here = os.path.dirname(__file__)
        rootdir = os.path.dirname(os.path.dirname(here))

        template_dir = os.path.join(rootdir, 'templates')
        public_dir = os.path.join(rootdir, 'public')
        
        config['extra_template_paths'] = ','.join([template_dir,
                config.get('extra_template_paths', '')])
        config['extra_public_paths'] = ','.join([public_dir,
                config.get('extra_public_paths', '')])

