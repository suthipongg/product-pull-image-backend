from dateutil.parser import parse as date_parse
from datetime import date, timedelta

class MGFuncs():
    @staticmethod
    def compute_pagination(total_hits, page, page_size):
        if page is None or page_size is None:
            return 0, None
        skip_count = (page - 1) * page_size
        page_count = max((total_hits + page_size - 1) // page_size, 1)
        return skip_count, {
            'currentPage': page,
            'pageSize': page_size,
            'totalPages': page_count,
            'totalItems': total_hits
        }
    

    @classmethod
    def convert_operator_to_mongo_query(cls, operator):
        if not isinstance(operator, dict):
            return operator
        elif 'ne' in operator:
            return {'$ne': operator['ne']}
        elif all(key in ['gte', 'lte', 'gt', 'lt'] for key in operator):
            return {
                f'${key}':  value if not isinstance(value, str)
                            else cls.convert_end_date_filter(date_parse(value)) if key == 'lte'
                            else date_parse(value)
                for key, value in operator.items()
            }
        elif 'regex' in operator:
            return {'$regex': operator['regex'], '$options': 'i'}
        else:
            raise ValueError('Invalid filter operator')
        
    @classmethod
    def convert_filter_to_mongo_query(cls, filter:dict):
        return {
            field: cls.convert_operator_to_mongo_query(operator) 
            for field, operator in filter.items()
        }


    @staticmethod
    def convert_projection_to_mongo_query(include:list=None, exclude:list=None):
        if include:
            projection = dict(map(lambda x: (x, 1), include))
            if '_id' not in include:
                projection.update({'_id': 0})
        else:
            projection = dict(map(lambda x: (x, 0), exclude))
        return projection


    @staticmethod
    def query_action(collection, query, projection, sort, skip_count, page_size):
        items = collection.find(query, projection)
        items = items.sort(sort) if sort else items
        items = items.skip(skip_count).limit(page_size)
        return items

    @classmethod
    def query_collection(cls, body, collection) -> tuple:
        body = body.model_dump()
        query = cls.convert_filter_to_mongo_query(body['filter'])
        projection = cls.convert_projection_to_mongo_query(body['include'], body['exclude'])
        sort = list(body['sort'].items())
        skip_count, pagination = cls.compute_pagination(collection.count_documents(query), body.get('page', None), body.get('pageSize', None))
        items = cls.query_action(collection, query, projection, sort, skip_count, body['pageSize'])
        return items, pagination 
    

    @staticmethod
    def aggregate_action(collection, pipeline: list, projection, sort, skip_count, page_size):
        if sort:
            pipeline.append({'$sort': sort})
        if skip_count:
            pipeline.append({'$skip': skip_count})
        if page_size:
            pipeline.append({'$limit': page_size})
        if projection:
            pipeline.append({'$project': projection})
        items = collection.aggregate(pipeline)
        return items

    @classmethod
    def aggregate_collection(cls, body:dict, collection, pipeline) -> tuple:
        match = cls.convert_filter_to_mongo_query(body['filter'])
        projection = cls.convert_projection_to_mongo_query(body['include'], body['exclude'])
        pipeline = pipeline(match)
        total_count = list(collection.aggregate(pipeline+[{"$count": "total_count"}]))
        skip_count, pagination = cls.compute_pagination(total_count[0]['total_count'] if total_count else 0, body.get('page', None), body.get('pageSize', None))
        items = cls.aggregate_action(collection, pipeline, projection, body['sort'], skip_count, body['pageSize'])
        return items, pagination 

    @staticmethod
    def convert_str_to_bool(value):
        result = None
        if isinstance(value, bool):
            result = value
        elif value is None:
            result = None
        elif isinstance(value, str):
            if value.lower() == 'true':
                result = True
            elif value.lower() == 'false':
                result = False
            else:
                result = None
        else:
            raise ValueError('Invalid value type')
        return result

    @classmethod
    def convert_filter_bool_field(cls, body:dict, field:str):
        value = cls.convert_str_to_bool(body.get(field, None))
        if value is None:
            body.pop(field, None)
        else:
            body[field] = value

    @classmethod
    def convert_end_date_filter(cls, date):
        return  date + timedelta(days=1) - timedelta(microseconds=1)

    @classmethod
    def preprocessing_date_filter(cls, filter:dict, date_field:str='timestamp'):
        if filter.get(date_field) is None:
            date_now = date.today()
            filter[date_field] = {
                'gte': str(date_now - timedelta(days=6)),
                'lte': str(date_now + timedelta(days=1))
            }
        return filter