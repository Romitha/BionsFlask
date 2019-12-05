from __future__ import unicode_literals
import datetime
import pprint
from io import StringIO
from urllib.request import urlopen

import chardet
from flask import logging

from src.models.ModelingModel import ModelingModel
from ..shared.JSONEncoder import JSONEncoder
from bson import ObjectId
from marshmallow import Schema, fields
# import built-in validators
from marshmallow.validate import Length, Range
from . import mongo
import json
import time
import pandas as pd
import csv
from socket import timeout


class ProjectModel:

    # class constructor
    def __init__(self, project_name, tags, description, raw_data_id, bind_collection_list, configuration_id,
                 primary_data_id, org_id):
        self.project_name = project_name
        self.tags = tags
        self.description = description
        self.raw_data_id = raw_data_id
        self.bind_collection_list = bind_collection_list
        self.configuration_id = configuration_id
        self.primary_data_id = primary_data_id
        self.org_id = org_id
        self.created_at = int(round(time.time() * 1000))
        self.updated_at = int(round(time.time() * 1000))
        self.deleted_at = ""

    def createProject(self):
        try:
            return_data = []
            raw_data = []
            # make raw data id object json
            for data_id in self.raw_data_id:
                raw_data.append(ObjectId(data_id))

            # ==============================================
            json_data = {
                "project_name": self.project_name,
                "tags": self.tags,
                "description": self.description,
                "raw_data_id": raw_data,
                "bind_collection_list": self.bind_collection_list,
                "configuration_id": self.configuration_id,
                "primary_data_id": self.primary_data_id,
                "org_id": ObjectId(self.org_id),
                "created_at": self.created_at,
                "updated_at": self.updated_at,
                "deleted_at": self.deleted_at,
            }
            # ==============================================
            project = mongo.db.project
            if not project.find_one({"project_name": self.project_name}):
                insert_data = project.insert_one(json_data).inserted_id
                if insert_data:
                    return_data = {
                        "status": 1,
                        "message": "Successfully Insert",
                        "data": json.loads(JSONEncoder().encode(insert_data))
                    }
                else:
                    return_data = {
                        "status": 0,
                        "message": "Something Wrong !",
                        "data": ""
                    }
            else:
                return_data = {
                    "status": 0,
                    "message": "You are trying to put duplicate data !",
                    "data": ""
                }
            return return_data

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def findOneProject(cls, project_id):
        try:
            return_data = []
            # ==============================================
            json_data = {
                '_id': ObjectId(project_id)
            }
            # ==============================================
            project = mongo.db.project
            if project.find_one(json_data):
                find_data = project.find_one(json_data)
                finalized_find_data = JSONEncoder().encode(find_data)
                if find_data:
                    return_data = {
                        "status": 1,
                        "message": "Successfully find a search",
                        "data": json.loads(finalized_find_data)
                    }
                else:
                    return_data = {
                        "status": 0,
                        "message": "Something Wrong !",
                        "data": ""
                    }
            else:
                return_data = {
                    "status": 0,
                    "message": "You are trying to get wrong data !",
                    "data": ""
                }
            return return_data

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def loadProjectConfiguration(cls, project_id):
        try:
            return_data = []
            # ==============================================
            json_data = {
                '_id': ObjectId(project_id)
            }
            # ==============================================
            project = mongo.db.project
            if project.find_one(json_data):
                find_data = project.find_one(json_data)
                finalized_find_data = JSONEncoder().encode(find_data['global_config'])

                if find_data:
                    return_data = {
                        "status": 1,
                        "message": "Successfully find a search",
                        "data": json.loads(finalized_find_data)
                    }
                else:
                    return_data = {
                        "status": 0,
                        "message": "Something Wrong !",
                        "data": ""
                    }
            else:
                return_data = {
                    "status": 0,
                    "message": "You are trying to get wrong data !",
                    "data": ""
                }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def findCollection(cls, collection_name):
        try:
            return_data = []
            project = mongo.db.list_collection_names()
            if project:
                return_data = {
                    "status": 1,
                    "message": project,
                    "data": ""
                }
            else:
                return_data = {
                    "status": 0,
                    "message": "You are trying to get wrong collection !",
                    "data": ""
                }
        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def deleteProject(cls, project_id):
        try:
            return_data = []
            # ==============================================
            json_data = {
                '_id': ObjectId(project_id)
            }
            # ==============================================
            project = mongo.db.project
            if project.find_one(json_data):
                result = project.delete_one(json_data).deleted_count
                if result:
                    return_data = {
                        "status": 1,
                        "message": "Successfully deleted",
                        "data": ""
                    }
                else:
                    return_data = {
                        "status": 0,
                        "message": "Something Wrong !",
                        "data": ""
                    }
            else:
                return_data = {
                    "status": 0,
                    "message": "You are trying to delete wrong data !",
                    "data": ""
                }
        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @staticmethod
    def myConverter(o):
        if isinstance(o, datetime.datetime):
            return o.__str__()

    @classmethod
    def updateProject(cls, project_id, project_name, tags, description, raw_data_id, bind_collection_list,
                      configuration_id, primary_data_id, org_id):
        try:
            return_data = []
            objForUpdate = {}
            # ==============================================
            if project_name:
                objForUpdate['project_name'] = project_name
            if tags:
                objForUpdate['tags'] = tags
            if description:
                objForUpdate['description'] = description
            if raw_data_id:
                objForUpdate['raw_data_id'] = raw_data_id
            if bind_collection_list:
                objForUpdate['bind_collection_list'] = bind_collection_list
            if configuration_id:
                objForUpdate['configuration_id'] = configuration_id
            if primary_data_id:
                objForUpdate['primary_data_id'] = primary_data_id
            if org_id:
                objForUpdate['org_id'] = ObjectId(org_id)
            objForUpdate['updated_at'] = int(round(time.time() * 1000))

            json_id = {
                '_id': ObjectId(project_id)
            }

            objForUpdate = {'$set': objForUpdate}
            # ==============================================
            project = mongo.db.project
            if project.find_one(json_id):
                update_data = project.update_one(json_id, objForUpdate)
                pprint.pprint(update_data)
                if update_data:
                    return_data = {
                        "status": 1,
                        "message": "Successfully Updated",
                        "data": ""
                    }
                else:
                    return_data = {
                        "status": 0,
                        "message": "Something Wrong !",
                        "data": ""
                    }
            else:
                return_data = {
                    "status": 0,
                    "message": "You are trying to update non related data !",
                    "data": ""
                }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def updateProjectConfiguration(cls, project_id, locale, separator, field_type, tokenize, header_place,
                                   date_time_field, missing_tokens, text_analysis, filter_terms):
        try:
            return_data = []
            objForUpdate = {'global_config': {
                'locale': ObjectId(locale),
                'separator': ObjectId(separator),
                'field_type': ObjectId(field_type),
                'tokenize': ObjectId(tokenize),
                'header_place': ObjectId(header_place),
                'date_time_field': ObjectId(date_time_field),
                'missing_tokens': missing_tokens,
                'text_analysis': ObjectId(text_analysis),
                'filter_terms': filter_terms
            }, 'updated_at': int(round(time.time() * 1000))}

            json_id = {
                '_id': ObjectId(project_id)
            }

            objForUpdate = {'$set': objForUpdate}
            # ==============================================
            project = mongo.db.project
            if project.find_one(json_id):
                update_data = project.update_one(json_id, objForUpdate)
                pprint.pprint(update_data)
                if update_data:
                    return_data = {
                        "status": 1,
                        "message": "Successfully Updated",
                        "data": ""
                    }
                else:
                    return_data = {
                        "status": 0,
                        "message": "Something Wrong !",
                        "data": ""
                    }
            else:
                return_data = {
                    "status": 0,
                    "message": "You are trying to update non related data !",
                    "data": ""
                }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def updateRawDataConfiguration(cls, id, project_id, raw_data_id, locale, separator, field_type, tokenize,
                                   header_place,
                                   date_time_field, missing_tokens, text_analysis, language_option, ta_tokenize, stop_words_removal, case_sensitive_option, filter_terms):
        try:
            return_data = []
            objForUpdate = {}
            # ==============================================
            if project_id:
                objForUpdate['project_id'] = ObjectId(project_id)
            if raw_data_id:
                objForUpdate['raw_data_id'] = ObjectId(raw_data_id)
            if locale:
                objForUpdate['locale'] = ObjectId(locale)
            if separator:
                objForUpdate['separator'] = ObjectId(separator)
            if field_type:
                objForUpdate['field_type'] = ObjectId(field_type)
            if tokenize:
                objForUpdate['tokenize'] = ObjectId(tokenize)
            if header_place:
                objForUpdate['header_place'] = ObjectId(header_place)
            if date_time_field:
                objForUpdate['date_time_field'] = ObjectId(date_time_field)
            if missing_tokens:
                objForUpdate['missing_tokens'] = missing_tokens
            if text_analysis:
                objForUpdate['text_analysis'] = ObjectId(text_analysis)
            if language_option:
                objForUpdate['language_option'] = ObjectId(language_option)
            if ta_tokenize:
                objForUpdate['ta_tokenize'] = ObjectId(ta_tokenize)
            if stop_words_removal:
                objForUpdate['stop_words_removal'] = ObjectId(stop_words_removal)
            if case_sensitive_option:
                objForUpdate['case_sensitive_option'] = ObjectId(case_sensitive_option)
            if filter_terms:
                objForUpdate['filter_terms'] = filter_terms
            objForUpdate['update_stage'] = True
            objForUpdate['updated_at'] = int(round(time.time() * 1000))

            json_id = {
                '_id': ObjectId(id)
            }

            objForUpdate = {'$set': objForUpdate}
            # ==============================================
            global_config = mongo.db.project_raw_data_global_config
            if global_config.find_one(json_id):
                update_data = global_config.update_one(json_id, objForUpdate)
                if update_data:
                    return_data = {
                        "status": 1,
                        "message": "Successfully Updated",
                        "data": ""
                    }
                else:
                    return_data = {
                        "status": 0,
                        "message": "Something Wrong !",
                        "data": ""
                    }
            else:
                return_data = {
                    "status": 0,
                    "message": "You are trying to update non related data !",
                    "data": ""
                }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def attachRawDataToProject(cls, project_id, raw_data_id, clone_collection_list_name):
        try:
            return_data = []
            raw_data = []
            # ==============================================
            for data_id in raw_data_id:
                raw_data.append(ObjectId(data_id))

            json_data = {
                "raw_data_id": raw_data_id,
                "bind_collection_list": clone_collection_list_name,
                "updated_at": int(round(time.time() * 1000))
            }
            json_id = {
                '_id': ObjectId(project_id)
            }
            # ==============================================
            project = mongo.db.project
            if project.find_one(json_id):
                update_data = project.update_one(json_id, {'$set': json_data})
                # pprint.pprint(update_data)
                if update_data:
                    return_data = {
                        "status": 1,
                        "message": "Successfully Updated",
                        "data": ""
                    }
                else:
                    return_data = {
                        "status": 0,
                        "message": "Something Wrong !",
                        "data": ""
                    }
            else:
                return_data = {
                    "status": 0,
                    "message": "You are trying to update non related data !",
                    "data": ""
                }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def removeAttachRawDataToProject(cls, project_id, raw_data_id):
        try:
            return_data = []
            raw_data = []
            # ==============================================
            for data_id in raw_data_id:
                raw_data.append(ObjectId(data_id))

            json_data = {
                "raw_data_id": raw_data_id,
                "updated_at": int(round(time.time() * 1000))
            }
            json_id = {
                '_id': ObjectId(project_id)
            }
            # ==============================================
            project = mongo.db.project
            if project.find_one(json_id):
                update_data = project.update_one(json_id, {'$set': json_data})
                # pprint.pprint(update_data)
                if update_data:
                    return_data = {
                        "status": 1,
                        "message": "Successfully Deleted",
                        "data": ""
                    }
                else:
                    return_data = {
                        "status": 0,
                        "message": "Something Wrong !",
                        "data": ""
                    }
            else:
                return_data = {
                    "status": 0,
                    "message": "You are trying to delete non related data !",
                    "data": ""
                }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def saveColumnDataTypes(cls, project_id, raw_data_id):
        print(raw_data_id)
        try:
            return_data = []
            raw_data = []
            final_collection = []
            json_id = {
                'project_id': ObjectId(project_id)
            }
            # ==============================================
            column_config = mongo.db.project_raw_data_column_config
            if column_config.find_one(json_id):
                column_config.remove(
                    {'project_id': ObjectId(project_id)})

            for data_id in raw_data_id:
                url = cls.findRawDataURL(data_id)
                # response = urlopen(url).read()
                try:
                    response = urlopen(url, timeout=10).read()
                except Exception as e:
                    return_data = {
                        "status": 0,
                        "message": 'Data of %s not retrieved because %s\nURL: %s' + str(e),
                        "data": ""
                    }
                result = chardet.detect(response)
                charenc = result['encoding']
                s = str(response, charenc)
                data = StringIO(s)
                df = pd.read_csv(data)

                columns = cls.getColumnsFromCSV(df)
                columns_type = cls.getDataTypeOfColumns(df, columns)

                # check CSV have the header or not
                # is_header = any(cell.isdigit() for cell in columns)
                has_header = csv.Sniffer().has_header(s)
                print(has_header)
                if has_header:
                    i = 1
                    for key, value in columns_type.items():
                        get_content = {
                            "name": key,
                            "type": ModelingModel.set_column_type(df[key]),
                            "lable": "",
                            "description": "",
                            "project_id": ObjectId(project_id),
                            "raw_data_id": ObjectId(data_id),
                            "url": url,
                            "place": i,
                            "created_at": int(round(time.time() * 1000)),
                            "updated_at": int(round(time.time() * 1000)),
                            "deleted_at": ""
                        }
                        final_collection.append(get_content)
                        insert_data = column_config.insert_one(get_content).inserted_id
                        if insert_data:
                            return_data = {
                                "status": 1,
                                "message": "Successfully Inserted",
                                "data": ""
                            }
                        else:
                            return_data = {
                                "status": 0,
                                "message": "Something Wrong !",
                                "data": ""
                            }
                        i = i + 1
                else:
                    print('not header-------------------------------------')
                    y = 1
                    for key, value in columns_type.items():
                        print(key)
                        print(y)
                        get_content = {
                            "name": "col" + str(y),
                            "type": "val" + str(y),
                            "lable": "",
                            "description": "",
                            "project_id": ObjectId(project_id),
                            "raw_data_id": ObjectId(data_id),
                            "url": url,
                            "place": y,
                            "created_at": int(round(time.time() * 1000)),
                            "updated_at": int(round(time.time() * 1000)),
                            "deleted_at": ""

                        }
                        y = y + 1
                        final_collection.append(get_content)
                        insert_data = column_config.insert_one(get_content).inserted_id
                        if insert_data:
                            return_data = {
                                "status": 1,
                                "message": "Successfully Inserted",
                                "data": ""
                            }
                        else:
                            return_data = {
                                "status": 0,
                                "message": "Something Wrong !",
                                "data": ""
                            }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def saveRawDataGlobalConfig(cls, project_id, global_config_raw_data_list):
        try:
            print("trigger saveRawDataGlobalConfig")
            return_data = []
            raw_data = []
            final_collection = []
            json_id = {
                'project_id': ObjectId(project_id)
            }

            print("global_config_raw_data_list")
            print(global_config_raw_data_list)
            print("END ----------------- global_config_raw_data_list")
            for data_id in global_config_raw_data_list:
                pipe = {
                    "$and": [
                        {"project_id": ObjectId(project_id)},
                        {"raw_data_id": ObjectId(data_id)},
                    ]
                }

                global_config = mongo.db.project_raw_data_global_config
                config_data = global_config.find_one(pipe)
                if not config_data:
                    print("try to insert in project_raw_data_global_config")
                    json_data = {
                        "project_id": ObjectId(project_id),
                        "raw_data_id": ObjectId(data_id),
                        'locale': ObjectId("5d9c1657d798ce0a7296112a"),
                        'separator': ObjectId("5d9c16bfd798ce0a7296112c"),
                        'field_type': ObjectId("5d9c34b15ff2dab84d772552"),
                        'tokenize': ObjectId("5d9c15702017522c4f163878"),
                        'header_place': ObjectId("5d9c35c6cce9247bdb350274"),
                        'date_time_field': ObjectId("5d9d61b7a960a1b9f61dc909"),
                        'missing_tokens': [],
                        'text_analysis': ObjectId("5d9d63dac411a9aa72c9045d"),
                        'language_option': ObjectId("5d9d64caff33ad817f114bf4"),
                        'ta_tokenize': ObjectId("5d9c15702017522c4f163878"),
                        'stop_words_removal': ObjectId("5d9d65d6f0ac8a2e8537a6a0"),
                        'case_sensitive_option': ObjectId("5d9d67197bcc46ec4e939287"),
                        'filter_terms': [],
                        'update_stage': False,
                        'created_at': int(round(time.time() * 1000)),
                        'updated_at': int(round(time.time() * 1000)),
                        'deleted_at': ""
                    }
                    insert_data = global_config.insert_one(json_data).inserted_id
                    if insert_data:
                        return_data = {
                            "status": 1,
                            "message": "Successfully Insert",
                            "data": json.loads(JSONEncoder().encode(insert_data))
                        }
                    else:
                        return_data = {
                            "status": 0,
                            "message": "Something Wrong !",
                            "data": ""
                        }
        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def getRawDataGlobalConfig(cls, project_id, raw_data_id):
        try:
            return_data = []
            json_id = {
                'project_id': ObjectId(project_id)
            }
            pipe = {
                "$and": [
                    {"project_id": ObjectId(project_id)},
                    {"raw_data_id": ObjectId(raw_data_id)},
                ]
            }
            global_config = mongo.db.project_raw_data_global_config

            if global_config.find_one(pipe):
                config_data = global_config.find_one(pipe)
                finalized_find_data = JSONEncoder().encode(config_data)
                if finalized_find_data:
                    return_data = {
                        "status": 1,
                        "message": "Successfully find a search raw data",
                        "data": json.loads(finalized_find_data)
                    }
                else:
                    return_data = {
                        "status": 0,
                        "message": "Something Wrong !",
                        "data": ""
                    }
            else:
                return_data = {
                    "status": 0,
                    "message": "You are trying to get wrong data !",
                    "data": ""
                }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def getColumnsFromCSV(cls, df):
        columns = []
        for col in df.columns:
            columns.append(col)
        return columns

    @classmethod
    def getDataTypeOfColumns(cls, df, columns):
        column_type = {}
        for name, dtype in df.dtypes.iteritems():
            column_type.update({str(name): str(dtype)})
        return column_type

    @classmethod
    def findRawDataURL(cls, raw_data_id):
        try:
            return_data = []
            # ==============================================
            json_data = {
                '_id': ObjectId(raw_data_id)
            }
            # ==============================================
            raw_data = mongo.db.raw_data
            if raw_data.find_one(json_data):
                find_data = raw_data.find_one(json_data)
                finalized_find_data = JSONEncoder().encode(find_data)
                if find_data:
                    return_data = find_data['path']

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def cloneCollectionToMongo(cls, collection_name, clone_name):
        try:
            return_data = []
            pipeline = [{"$match": {}},
                        {"$out": clone_name},
                        ]
            clone_collection = mongo.db[collection_name].aggregate(pipeline)
            pprint.pprint(clone_collection)
            if clone_collection:
                return_data = {
                    "status": 1,
                    "message": "Successfully Cloned ->" + collection_name,
                    "data": ""
                }
            else:
                return_data = {
                    "status": 0,
                    "message": "Something Wrong ! -> " + collection_name,
                    "data": ""
                }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def getPaginateProject(cls, page_size, page_num):
        try:
            return_data = []

            # ==============================================
            skips = page_size * (page_num - 1)
            cursor = mongo.db.project.find().skip(skips).limit(page_size)
            row_count = cursor.count()
            page_count = (row_count + page_size - 1) // page_size
            data = [x for x in cursor]
            # ==============================================
            data_encode = JSONEncoder().encode(data)
            final_data = json.loads(data_encode)

            return_data = {
                "status": 1,
                "message": "Raw Data List with pagination",
                "data": {
                    "current_page": page_num,
                    "data": final_data,
                    "per_page": page_size,
                    "total": row_count,
                    "page_count": page_count
                }
            }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def getPaginateProjectByOrganization(cls, page_size, page_num, org_id):
        try:
            return_data = []
            skips = page_size * (page_num - 1)
            # ==============================================
            pipeline = [
                {
                    "$lookup": {
                        "from": "organization",
                        "localField": "org_id",
                        "foreignField": "_id",
                        "as": "org"
                    }
                },
                {
                    "$match": {
                        "org_id": ObjectId(org_id)
                    }
                },
                {
                    "$sort": {
                        "order_number": -1
                    }
                },
                {
                    "$count": "raw_data_count"
                },
                {
                    "$facet": {
                        "data": [
                            {
                                "$skip": skips
                            },
                            {
                                "$limit": page_size
                            }
                        ]
                    }
                }
            ]
            count_pipeline = [
                {
                    "$lookup": {
                        "from": "organization",
                        "localField": "org_id",
                        "foreignField": "_id",
                        "as": "org"
                    }
                },
                {
                    "$match": {
                        "org_id": ObjectId(org_id)
                    }
                },
                {
                    "$count": "data_count"
                }
            ]
            raw_count_cursor = mongo.db.raw_data.aggregate(count_pipeline)
            raw_count_data = [x for x in raw_count_cursor]
            final_raw_data_count = raw_count_data[0]['data'][0]['data_count']
            page_count = (final_raw_data_count + page_size - 1) // page_size

            cursor = mongo.db.raw_data.aggregate(pipeline)
            data = [x for x in cursor]
            # ==============================================
            data_encode = JSONEncoder().encode(data)
            final_data = json.loads(data_encode)

            return_data = {
                "status": 1,
                "message": "Raw Data List with pagination",
                "data": {
                    "current_page": page_num,
                    "data": final_data,
                    "per_page": page_size,
                    "total": final_raw_data_count,
                    "page_count": page_count
                }
            }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def getProjectListByOrganization(cls, org_id):
        try:
            return_data = []
            # ==============================================
            pipeline = [
                {
                    "$match": {
                        "org_id": ObjectId(org_id)
                    }
                }
            ]

            cursor = mongo.db.project.aggregate(pipeline)
            data = [x for x in cursor]
            # ==============================================
            data_encode = JSONEncoder().encode(data)
            final_data = json.loads(data_encode)

            return_data = {
                "status": 1,
                "message": "Raw Data List with pagination",
                "data": {
                    "current_page": 1,
                    "data": final_data,
                    "per_page": 0,
                    "total": 0,
                    "page_count": 1
                }
            }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data

    @classmethod
    def getPaginateRawDataByProject(cls, page_size, page_num, project_id):
        try:
            return_data = []
            skips = page_size * (page_num - 1)
            # ==============================================
            pipeline = [
                {
                    "$lookup": {
                        "from": "project",
                        "localField": "_id",
                        "foreignField": "raw_data_id",
                        "as": "project_data"
                    }
                },
                {
                    "$unwind": "$project_data"
                },
                {
                    "$match": {
                        "project_data._id": ObjectId(project_id)
                    }
                },
                {
                    "$sort": {
                        "order_number": -1
                    }
                },
                {
                    "$facet": {
                        "data": [
                            {
                                "$skip": skips
                            },
                            {
                                "$limit": page_size
                            }
                        ]
                    }
                }
            ]
            count_pipeline = [
                {
                    "$lookup": {
                        "from": "project",
                        "localField": "_id",
                        "foreignField": "raw_data_id",
                        "as": "project_data"
                    }
                },
                {
                    "$unwind": "$project_data"
                },
                {
                    "$match": {
                        "project_data._id": ObjectId(project_id)
                    }
                },
                {
                    "$count": "data_count"
                }

            ]
            raw_count_cursor = mongo.db.raw_data.aggregate(count_pipeline)
            count_data = [x for x in raw_count_cursor]
            final_data_count = count_data[0]['data_count']
            page_count = (final_data_count + page_size - 1) // page_size

            cursor = mongo.db.raw_data.aggregate(pipeline)
            # ==============================================
            data_encode = JSONEncoder().encode([x for x in cursor])
            final_data = json.loads(data_encode)

            return_data = {
                "status": 1,
                "message": "Raw Data List with pagination",
                "data": {
                    "current_page": page_num,
                    "data": final_data[0]['data'],
                    "per_page": page_size,
                    "total": final_data_count,
                    "page_count": page_count
                }
            }

        except Exception as e:
            return_data = {
                "status": 0,
                "message": "Exception seen: " + str(e),
                "data": ""
            }
        return return_data


class projectSchema(Schema):
    project_name = fields.Str(required=True, validate=Length(max=60))
    tags = fields.List(fields.String, required=False)
    description = fields.Str(required=True, validate=Length(max=1000))
    raw_data_id = fields.List(fields.String, required=False)
    bind_collection_list = fields.List(fields.String, required=False)
    configuration_id = fields.Str(required=True)
    primary_data_id = fields.List(fields.String, required=False)
    org_id = fields.Str(required=True)


class updateProjectSchema(Schema):
    id = fields.Str(required=True)
    project_name = fields.Str(required=True, validate=Length(max=60))
    tags = fields.List(fields.String, required=False)
    description = fields.Str(required=True, validate=Length(max=1000))
    raw_data_id = fields.List(fields.String, required=False)
    bind_collection_list = fields.List(fields.String, required=False)
    configuration_id = fields.Str(required=True)
    primary_data_id = fields.List(fields.String, required=False)
    org_id = fields.Str(required=True)


class updateAttachRawDataConfigSchema(Schema):
    id = fields.Str(required=True)
    project_id = fields.Str(required=True)
    raw_data_id = fields.Str(required=True)
    locale = fields.Str(required=True)
    separator = fields.Str(required=True)
    field_type = fields.Str(required=True)
    tokenize = fields.Str(required=True)
    header_place = fields.Str(required=True)
    date_time_field = fields.Str(required=True)
    missing_tokens = fields.List(fields.String, required=False)
    text_analysis = fields.Str(required=True)
    language_option = fields.Str(required=True)
    ta_tokenize = fields.Str(required=True)
    stop_words_removal = fields.Str(required=True)
    case_sensitive_option = fields.Str(required=True)
    filter_terms = fields.List(fields.String, required=False)


class updateProjectConfigurationSchema(Schema):
    id = fields.Str(required=True)
    locale = fields.Str(required=True, validate=Length(max=60))
    separator = fields.Str(required=True)
    field_type = fields.Str(required=True, validate=Length(max=1000))
    tokenize = fields.Str(required=True, validate=Length(max=1000))
    header_place = fields.Str(required=True, validate=Length(max=1000))
    date_time_field = fields.Str(required=True, validate=Length(max=1000))
    missing_tokens = fields.List(fields.String, required=False)
    text_analysis = fields.Str(required=True)
    filter_terms = fields.List(fields.String, required=False)


class attachRawDataToProjectSchema(Schema):
    id = fields.Str(required=True)
    option = fields.Int(required=True)
    raw_data_id = fields.List(fields.String, required=False)


class findOneProjectSchema(Schema):
    id = fields.Str(required=True)


class findRawDataConfigSchema(Schema):
    project_id = fields.Str(required=True)
    raw_data_id = fields.Str(required=True)


class deleteAttachRawDataSchema(Schema):
    project_id = fields.Str(required=True)
    raw_data_id = fields.Str(required=True)


class listOfProjectByOrg(Schema):
    id = fields.Str(required=True)


class paginateProjectSchema(Schema):
    page_size = fields.Int(required=True)
    page_num = fields.Int(required=True)


class paginationWithAttachDataProject(Schema):
    page_size = fields.Int(required=True)
    page_num = fields.Int(required=True)
    project_id = fields.Str(required=True, validate=Length(max=1000))
