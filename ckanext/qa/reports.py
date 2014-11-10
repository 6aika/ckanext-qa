from collections import namedtuple
from sqlalchemy import or_, and_, func, desc
import ckan.model as model
import ckan.plugins as p
import ckan.lib.dictization.model_dictize as model_dictize
import json
import logging

log = logging.getLogger(__name__)

resource_dictize = model_dictize.resource_dictize

def convert_sqlalchemy_result_to_DictObj(result):
    return DictObj(zip(result.keys(), result))

class DictObj(dict):
    """\
    Like a normal Python dictionary, but allows keys to be accessed as
    attributes. For example:
    ::
        >>> person = DictObj(firstname='James')
        >>> person.firstname
        'James'
        >>> person['surname'] = 'Jones'
        >>> person.surname
        'Jones'
    """
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError('No such attribute %r'%name)

    def __setattr__(self, name, value):
        raise AttributeError(
            'You cannot set attributes of this DictObject directly'
        )


def five_stars(id=None):
    """
    Return a list of dicts: 1 for each dataset that has an openness score.

    Each dict is of the form:
        {'name': <string>, 'title': <string>, 'openness_score': <int>}
    """
    if id:
        pkg = model.Package.get(id)
        if not pkg:
            return "Not found"

    # take the maximum openness score among dataset resources to be the
    # overall dataset openness core
    query = model.Session.query(model.Package.name, model.Package.title,
                                model.Resource.id,
                                model.TaskStatus.value.label('value'))\
        .join(model.ResourceGroup, model.Package.id == model.ResourceGroup.package_id)\
        .join(model.Resource)\
        .join(model.TaskStatus, model.TaskStatus.entity_id == model.Resource.id)\
        .filter(model.TaskStatus.key==u'openness_score')\
        .group_by(model.Package.name, model.Package.title, model.Resource.id, model.TaskStatus.value)\
        .distinct()

    if id:
        query = query.filter(model.Package.id == pkg.id)

    results = []
    for row in query:
        results.append({
            'name': row.name,
            'title': row.title + u' ' + row.id,
            'openness_score': row.value
        })

    return results

# from datagovuk
def dataset_five_stars(dataset_id):
    '''For a dataset, return an overall five star score plus textual details of
    why it merits that.
    Of the resources, it returns details of the one with the highest QA score.
    Returns a dict of {'name': <package name>,
                       'title': <package title>,
                       'id': <resource id>,
                       'last_updated': <date of last update of openness score
                                        (datetime)>,
                       'value': <openness score (int)>,
                       'reason': <text describing score reasoning>,
                       'is_broken': <whether the link is broken (bool)>,
                       'format': <the detected file format>,
                       }
    '''

    import ckan.model as model
    # Run a query to choose the most recent, highest qa score of all resources in this dataset.
    query = model.Session.query(model.Package.name, model.Package.title, model.Resource.id, model.TaskStatus.last_updated.label('last_updated'), model.TaskStatus.value.label('value'), model.TaskStatus.error.label('error')) \
        .join(model.ResourceGroup, model.Package.id == model.ResourceGroup.package_id) \
        .join(model.Resource) \
        .join(model.TaskStatus, model.TaskStatus.entity_id == model.Resource.id) \
        .filter(model.TaskStatus.task_type==u'qa') \
        .filter(model.TaskStatus.key==u'status') \
        .filter(model.Package.id == dataset_id) \
        .filter(model.Resource.state==u'active') \
        .order_by(desc(model.TaskStatus.value)) \
        .order_by(desc(model.TaskStatus.last_updated)) \

    report = query.first()
    if not report:
        pkg = model.Package.get(dataset_id)
        if pkg:
            num_resources = model.Session.query(model.ResourceGroup) \
                .join(model.Resource) \
                .filter(model.ResourceGroup.package_id == dataset_id) \
                .filter(model.Resource.state==u'active') \
                .count()
            if num_resources == 0:
                # Package has no resources, so gets 0 stars
                return {'name': pkg.name,
                        'title': pkg.title,
                        'id': None,
                        'last_updated': None,
                        'value': 0,
                        'reason': 'No data resources, so scores 0.'}
        # Package hasn't been rated yet
        return None

    # Transfer to a DictObj - I don't trust the SqlAlchemy result to
    # exist for the remainder of the request, although it's not disappeared
    # in practice.
    result = convert_sqlalchemy_result_to_DictObj(report)
    result['value'] = int(report.value)
    try:
        result.update(json.loads(result['error']))
    except ValueError, e:
        log.error('QA status "error" should have been in JSON format, but found: "%s" %s', result['error'], e)
        result['reason'] = 'Could not display reason due to a system error'
    del result['error']

    return result

def resource_five_stars(id):
    """
    Return a dict containing the QA results for a given resource

    Each dict is of the form:
        {'openness_score': <int>, 'openness_score_reason': <string>, 'failure_count': <int>}
    """
    if id:
        r = model.Resource.get(id)
        if not r:
            return {}  # Not found

    context = {'model': model, 'session': model.Session}
    data = {'entity_id': r.id, 'task_type': 'qa'}

    try:
        data['key'] = 'openness_score'
        status = p.toolkit.get_action('task_status_show')(context, data)
        openness_score = int(status.get('value'))
        openness_score_updated = status.get('last_updated')

        data['key'] = 'openness_score_reason'
        status = p.toolkit.get_action('task_status_show')(context, data)
        openness_score_reason = status.get('value')
        openness_score_reason_updated = status.get('last_updated')

        data['key'] = 'openness_score_failure_count'
        status = p.toolkit.get_action('task_status_show')(context, data)
        openness_score_failure_count = int(status.get('value'))
        openness_score_failure_count_updated = status.get('last_updated')

        last_updated = max( 
            openness_score_updated,
            openness_score_reason_updated,
            openness_score_failure_count_updated )

        result = {
            'openness_score': openness_score,
            'openness_score_reason': openness_score_reason,
            'openness_score_failure_count': openness_score_failure_count,
            'openness_score_updated': openness_score_updated,
            'openness_score_reason_updated': openness_score_reason_updated,
            'openness_score_failure_count_updated': openness_score_failure_count_updated,
            'openness_updated': last_updated
        }
    except p.toolkit.ObjectNotFound:
        result = {}

    return result


def broken_resource_links_by_dataset():
    """
    Return a list of named tuples, one for each dataset that contains
    broken resource links (defined as resources with an openness score of 0).

    The named tuple is of the form:
        (name (str), title (str), resources (list of dicts))
    """
    query = model.Session.query(model.Package.name, model.Package.title, model.Resource)\
        .join(model.ResourceGroup, model.Package.id == model.ResourceGroup.package_id)\
        .join(model.Resource)\
        .join(model.TaskStatus, model.TaskStatus.entity_id == model.Resource.id)\
        .filter(model.TaskStatus.key == u'openness_score')\
        .filter(model.TaskStatus.value == u'0')\
        .distinct()

    context = {'model': model, 'session': model.Session}
    results = {}
    for name, title, resource in query:
        resource = resource_dictize(resource, context)

        data = {'entity_id': resource['id'], 'task_type': 'qa', 'key': 'openness_score_reason'}
        status = p.toolkit.get_action('task_status_show')(context, data)
        resource['openness_score_reason'] = status.get('value')

        if name in results:
            results[name].resources.append(resource)
        else:
            DatasetTuple = namedtuple('DatasetTuple', ['name', 'title', 'resources'])
            results[name] = DatasetTuple(name, title or name, [resource])

    return results.values()


def broken_resource_links_by_dataset_for_organisation(organisation_id):
    result = _get_broken_resource_links(organisation_id)
    if result:
        return {
            'id': result.keys()[0][1],
            'title': result.keys()[0][0],
            'packages': result.values()[0]
        }
    else:
        return {
            'id': None,
            'title': None,
            'packages': []
        }


def organisations_with_broken_resource_links_by_name():
    result = _get_broken_resource_links().keys()
    result.sort()
    return result


def organisations_with_broken_resource_links():
    return _get_broken_resource_links()


def _get_broken_resource_links(organisation_id=None):
    organisation_id = None

    query = model.Session.query(model.Package.name, model.Package.title,
                                model.PackageExtra.value, model.Resource)\
        .join(model.PackageExtra)\
        .join(model.ResourceGroup, model.Package.id == model.ResourceGroup.package_id)\
        .join(model.Resource)\
        .join(model.TaskStatus, model.TaskStatus.entity_id == model.Resource.id)\
        .filter(model.TaskStatus.key == u'openness_score')\
        .filter(model.TaskStatus.value == u'0')\
        .filter(or_(
            and_(model.PackageExtra.key=='published_by',
                 model.PackageExtra.value.like('%%[%s]' % (organisation_id is None and '%' or organisation_id))),
            and_(model.PackageExtra.key=='published_via',
                 model.PackageExtra.value.like('%%[%s]' % (organisation_id is None and '%' or organisation_id))),
            )\
        )\
        .distinct()

    context = {'model': model, 'session': model.Session}
    data = []
    for row in query:
        resource = resource_dictize(row.Resource, context)
        task_data = {'entity_id': resource['id'], 'task_type': 'qa', 'key': 'openness_score_reason'}
        status = p.toolkit.get_action('task_status_show')(context, task_data)
        resource['openness_score'] = u'0'
        resource['openness_score_reason'] = status.get('value')

        data.append([row.name, row.title, row.value, resource])

    return _collapse(data, [_extract_publisher, _extract_dataset])


def _collapser(data, key_func=None):
    result = {}
    for row in data:
        if key_func:
            row = key_func(row)
        key = row[0]
        if len(row) == 2:
            row = row[1]
        else:
            row = row[1:]
        if key in result:
            result[key].append(row)
        else:
            result[key] = [row]
    return result


def _collapse(data, fn):
    first = _collapser(data, fn[0])
    result = {}
    for k, v in first.items():
        result[k] = _collapser(v, fn[1])
    return result


def _extract_publisher(row):
    """
    Extract publisher info from a query result row.
    Each row should be a list of the form [name, title, value, Resource]

    Returns a list of the form:

        [<publisher tuple>, <other elements in row tuple>]
    """
    publisher = row[2]
    parts = publisher.split('[')
    try:
        pub_parts = (parts[0].strip(), parts[1][:-1])
    except:
        raise Exception('Could not get the ID from %r' % publisher)
    else:
        return [pub_parts] + [row[0], row[1], row[3]]


def _extract_dataset(row):
    """
    Extract dataset info form a query result row.
    Each row should be a list of the form [name, title, Resource]

    Returns a list of the form:

        [(name, title), Resource]
    """
    return [(row[0], row[1]), row[2]]
