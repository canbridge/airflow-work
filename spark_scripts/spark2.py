from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructField, StructType, StringType

import json

spark = SparkSession.builder.appName('liu').\
        config("spark.mongodb.input.uri", 'mongodb://developer:Threfo0998@10.0.0.90/data.foo?authSource=admin').\
        config("spark.mongodb.output.uri", 'mongodb://developer:Threfo0998@10.0.0.90/data.fo1?authSource=admin').\
        getOrCreate()
json_source = 'cosn://data/yf/yf_resumes_test'
df = spark.read.json(json_source)

target_string_keys = ['work_position', 'phone', 'grad_time', 'major', 'work_year', 'college_rank', 'college_type', 'work_company',
               'work_start_time',  'marital_status', 'name',
               'resume_integrity', 'cont_basic_info', 'resume_type',  'hukou_address',
               'raw_text', 'hometown_address', 'college', 'expect_salary', 'cont_my_desc', 'gender',
               'email', 'degree', 'age', 'birth_year', 'city', 'city_dist', 'expect_city',
               'expect_industry', 'expect_work_type', 'job_state', 'job_title', 'last_time',
               'province', 'user_id'
               ]

target_list_keys = ['cert_objs', 'lang_objs', 'project_exps', 'education_objs',
                    'training_objs', 'job_exp_objs', 'skills_objs']

string_map = {
        "work_position":{
            "bello": "work_position",
            "yf": "",},

        "phone":{
            "bello": "phone",
            "yf": "telephone",},

        "grad_time":{
            "bello": "grad_time",
            "yf": "",},

        "major":{
            "bello": "major",
            "yf": "",},

        "work_year":{
            "bello": "work_year",
            "yf": "jobYear",},

        "college_rank":{
            "bello": "college_rank",
            "yf": "",},

        "college_type":{
            "bello": "college_type",
            "yf": "",},

        "work_company":{
            "bello": "work_company",
            "yf": "",},

        "work_start_time":{
            "bello": "work_start_time",
            "yf": "",},

        "marital_status":{
            "bello": "",
            "yf": "",},

        "name":{
            "bello": "name",
            "yf": "name",},


        "resume_integrity":{
            "bello": "resume_integrity",
            "yf": "",},


        "cont_basic_info":{
            "bello": "cont_basic_info",
            "yf": "",},

        "resume_type":{
            "bello": "resume_type",
            "yf": "",},

        "raw_text":{
            "bello": "raw_text",
            "yf": "",},

        "hometown_address":{
            "bello": "hometown_address",
            "yf": "",},

        "college":{
            "bello": "college",
            "yf": "",},
        "expect_salary":{
            "bello": "expect_salary",
            "yf": "expectSalary",},

        "cont_my_desc":{
            "bello": "cont_my_desc",
            "yf": "selfEvaluate",},


        "gender":{
            "bello": "gender",
            "yf": "sex",},

        "email":{
            "bello": "email",
            "yf": "email",},

        "degree":{
            "bello": "degree",
            "yf": "",},

        "age":{
            "bello": "",
            "yf": "age",},

        "birth_year":{
            "bello": "",
            "yf": "birthYear",},

        "city":{
            "bello": "",
            "yf": "city",},

        "city_dist":{
            "bello": "",
            "yf": "cityDist",},

        "expect_city":{
            "bello": "",
            "yf": "",},

        "expect_industry":{
            "bello": "",
            "yf": "expectIndustry",},

        "expect_work_type":{
            "bello": "",
            "yf": "expectWorkType",},

        "job_state":{
            "bello": "",
            "yf": "jobState",},


        "job_title":{
            "bello": "",
            "yf": "jobTitle",},

        "last_time":{
            "bello": "",
            "yf": "lastTime",},

        "province":{
            "bello": "",
            "yf": "province",},

        "user_id":{
            "bello": "",
            "yf": "userID",},
        }

yf_list_map = {
    "lang_objs": "languagesList",
    "project_exps": "projectList",
    "education_objs": "eduList",
    "training_objs": "trainList",
    "job_exp_objs": "workList",
    "skills_objs": "skillList",
        }

bello_list_map = {
}

map_2 = {

        "cert_objs": {
            "bello": "cert_objs",
            "yf": "",
            },


        "lang_objs": {
            "bello": "lang_objs",
            "yf": "languagesList",
            },

        "project_exps": {
            "bello": "proj_exp_objs",
            "yf": "projectList",
            },

        "education_objs": {
            "bello": "education_objs",
            "yf": "eduList",
            },

        "training_objs": {
            "bello": "training_objs",
            "yf": "trainList",
            },

        "job_exp_objs": {
            "bello": "job_exp_objs",
            "yf": "workList",
            },

        "skills_objs": {
            "bello": "skills_objs",
            "yf": "skillList",
            },
}

special_map = {
        "hukou_address": "hukouprovince+hukoucity",
                    }

list_map = {
          "lang_objs": {
            "language_name": {"bello": "language_name", "yf": "languageName"},
            "language_read_write": {"bello":"language_read_write", "yf":"readWriteSkill"},
            "language_listen_speak": {"bello": "language_listen_speak", "yf": "hearSpeakSkill"},
            },
          "project_exps": {
            "proj_content": {"bello": "proj_content", "yf": "projectDesc"},
            "end_date": {"bello":"end_date", "yf":"endTime"},
            "proj_resp": {"bello": "proj_resp", "yf": "responsibilityDesc"},
            "proj_name": {"bello": "proj_name", "yf": "projectName"},
            "start_date": {"bello": "start_date", "yf": "startTime"},
            "dev_tools": {"bello": "", "yf": "dev_tools"},
            "software": {"bello": "", "yf": "software"},
            "user_id": {"bello": "", "yf": "userId"},
            },

          "education_objs": {
            "start_date": {"bello": "start_date", "yf": "startTime"},
            "edu_college_rank": {"bello": "edu_college_rank", "yf": ""},
            "edu_college_type": {"bello": "edu_college_type", "yf": ""},
            "edu_college": {"bello": "edu_college", "yf": "schoolName"},
            "edu_degree": {"bello": "edu_degree", "yf": ""},
            "edu_major": {"bello": "edu_major", "yf": "specialty"},
            "end_date": {"bello": "end_date", "yf": "endTime"},
            "edu_degree_norm": {"bello": "edu_degree_norm", "yf": ""},
            "edu_recruit": {"bello": "edu_recruit", "yf": ""},
            "education": {"bello": "", "yf": "education"},
            },
       "training_objs":{
            "start_date": {"bello": "start_date", "yf":"startTime"},
            "end_date": {"bello": "end_date", "yf":"endTime"},
            "train_cont": {"bello": "train_cont", "yf":""},
            "train_name": {"bello": "", "yf":"train_name"},
            "machinery": {"bello": "", "yf":"machinery"},
            "certificate_name": {"bello": "", "yf":"certificate_name"},
            "address": {"bello": "", "yf":"address"},
            },
       "job_exp_objs":{
            "start_date": {"bello": "start_date", "yf":"startTime"},
            "job_content": {"bello": "job_content", "yf":"workDesc"},
            "job_cpy": {"bello": "job_cpy", "yf":"compName"},
            "job_duration": {"bello": "job_duration", "yf":""},
            "job_positaion": {"bello": "job_positaion", "yf":"jobTitle"},
            "comp_industry": {"bello": "", "yf":"compIndustry"},
            "comp_property": {"bello": "", "yf":"compProperty"},
            "comp_size": {"bello": "", "yf":"compSize"},
            "salary": {"bello": "", "yf":"salary"},
            },
    }


def change_yf_list_key(target, origin):
    for list_key in target_list_keys:
        eles = []
        target[list_key] = eles
        yf_list_k =  yf_list_map.get(list_key)
        if not yf_list_k:
            continue
        yf_list_v = origin.get(yf_list_k, [])
        in_eles = list_map.get(list_key, {})
        if in_eles:
            yf_in_keys = {}
            for target_v, in_v in in_eles.items():
                yf_in_keys[target_v] = in_v.get('yf', '')

            for i in yf_list_v:
                ele = {}
                for target_in_key, yf_in_key in yf_in_keys.items():
                    ele[target_in_key] = i.get(yf_in_key, '')
                if ele:
                    eles.append(ele)

def change_str_key(target, origin, source):
    for target_k, source_k in string_map.items():
        target[target_k] = ''
        yf_s_key = source_k.get(source)
        if yf_s_key:
            target[target_k] = origin.get(yf_s_key)

def change_yf_key(entity):
    origin = json.loads(entity)
    target = {}
    target['hukou_address'] = origin.get('hukouprovince', '') + origin.get('hukoucity', '')
    change_str_key(target, origin, 'yf')
    change_yf_list_key(target, origin)
    return json.dumps(target)

jdf = df.toJSON()
mjdf = jdf.map(change_yf_key)

print(1111111)
target = 'cosn://data/yf/yf_resumes_test_tar2'
# mjdf.saveAsTextFile(targetfrom pyspark.sql import SparkSession, Row
