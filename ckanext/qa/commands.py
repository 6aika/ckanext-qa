import datetime
import json
import requests
import urlparse
import logging
from pylons import config

import ckan.plugins as p


REQUESTS_HEADER = {'content-type': 'application/json'}

class CkanApiError(Exception):
    pass


class QACommand(p.toolkit.CkanCommand):
    """
    QA analysis of CKAN resources

    Usage::

        paster qa [options] update [dataset name/id]
           - QA analysis on all resources in a given dataset, or on all
           datasets if no dataset given

        paster qa clean
            - Remove all package score information

    The commands should be run from the ckanext-qa directory and expect
    a development.ini file to be present. Most of the time you will
    specify the config explicitly though::

        paster qa update --config=<path to CKAN config file>
    """
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 2
    min_args = 0

    def command(self):
        """
        Parse command line arguments and call appropriate method.
        """
        if not self.args or self.args[0] in ['--help', '-h', 'help']:
            print QACommand.__doc__
            return

        cmd = self.args[0]
        self._load_config()

        # Now we can import ckan and create logger, knowing that loggers
        # won't get disabled
        self.log = logging.getLogger('ckanext.qa')

        from ckan import model
        from ckan.model.types import make_uuid

        # import tasks after load config so CKAN_CONFIG evironment variable
        # can be set
        import tasks

        user = p.toolkit.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )

        self.site_url = config.get('ckan.site_url_internally') or config.get('ckan.site_url')

        context = json.dumps({
            'site_url': self.site_url,
            'apikey': user.get('apikey'),
            'username': user.get('name'),
        })

        if cmd == 'update':
            for package in self._package_list():
                self.log.info('QA on dataset being added to Celery queue "%s": %s (%d resources)' % \
                              (self.options.queue, package.get('name'),len(package.get('resources', []))))

                data = json.dumps(package)
                task_id = make_uuid()
                tasks.update_package.apply_async(args=[context, data],
                                                 task_id=task_id)

        elif cmd == 'clean':
            self.log.error('Command "%s" not implemented' % (cmd,))

        else:
            self.log.error('Command "%s" not recognized' % (cmd,))

    def make_post(self, url, data):
            headers = {'Content-type': 'application/json',
                       'Accept': 'text/plain'}
            return requests.post(url, data=json.dumps(data), headers=headers)

    def _package_list(self):
        """
        Generate the package dicts as declared in self.args.

        Make API calls for the packages declared in self.args, and generate
        the package dicts.

        If no packages are declared in self.args, then retrieve all the
        packages from the catalogue.
        """
        api_url = urlparse.urljoin(self.site_url, 'api/action')
        if len(self.args) > 1:
            for id in self.args[1:]:
                # try arg as a group name
                url = api_url + '/member_list'
                self.log.info('Trying as a group "%s" at URL: %r', id, url)
                data = {'id': id,
                        'object_type': 'package',
                        'capacity': 'public'}
                response = requests.post(url, data=json.dumps(data), headers=REQUESTS_HEADER)
                if response.status_code == 200:
                    package_tuples = json.loads(response.text).get('result')
                    package_names = [pt[0] for pt in package_tuples]
                else:
                    # must be a package id
                    package_names = [id]
                for package_name in sorted(package_names):
                    data = json.dumps({'id': unicode(package_name)})
                    url = api_url + '/package_show'
                    response = requests.post(url, data, headers=REQUESTS_HEADER)
                    if response.status_code == 403:
                        self.log.warning('Package "%s" is in the group but '
                                         'returned %i error, so skipping.' % \
                                         (package_name, response.status_code))
                        continue
                    if not response.ok:
                        err = ('Failed to get package %s from url %r: %s %s' %
                               (package_name, url, response.status_code, response.error))
                        self.log.error(err)
                        raise CkanApiError(err)
                    yield json.loads(response.content).get('result')
        else:
            page, limit = 1, 10
            while True:
                url = api_url + '/current_package_list_with_resources'
                response = requests.post(url,
                                         json.dumps({'page': page,
                                                     'limit': limit,
                                                     'order_by': 'name'}),
                                         headers=REQUESTS_HEADER)
                if not response.ok:
                    err = ('Failed to get package list with resources from url %r: %s %s' %
                           (url, response.status_code, response.error))
                    self.log.error(err)
                    raise CkanApiError(err)
                chunk = json.loads(response.content).get('result')
                if not chunk:
                    break
                for package in chunk:
                    yield package
                page += 1
