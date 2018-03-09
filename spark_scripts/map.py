basic_map = {
        "name": {
            "bello": "name",
            "yf": "name", },

        "email": {
            "bello": "email",
            "yf": "email", },

        "mobile": {
            "bello": "phone",
            "yf": "telephone", },

        "graduation_time": {
            "bello": "grad_time",
            "yf": "", },

        "degree": {
            "bello": "degree",
            "yf": "", },

        "major": {
            "bello": "major",
            "yf": "", },

        "nationality": {
            "bello": "",
            "yf": "", },

        "hukou": {
            "bello": "hukou_address",
            "yf": "hukouprovince", },

        "address": {
            "bello": "hometown_address",
            "yf": "", },

        "college": {
            "bello": "college",
            "yf": "", },

        "city": {
            "bello": "",
            "yf": "city", },

        "city_dist": {
            "bello": "",
            "yf": "cityDist", },

        "province": {
            "bello": "",
            "yf": "province", },

        "college_rank": {
            "bello": "college_rank",
            "yf": "", },

        "college_type": {
            "bello": "college_type",
            "yf": "", },

        "current_salary": {
            "bello": "",
            "yf": "", },

        "expected_salary_min": {
            "bello": "",
            "yf": "", },

        "expected_salary_max": {
            "bello": "",
            "yf": "", },

        "age": {
            "bello": "",
            "yf": "age", },

        "gender": {
            "bello": "gender",
            "yf": "sex", },

        "title": {
            "bello": "work_position",
            "yf": "jobTitle", },

        "company": {
            "bello": "work_company",
            "yf": "", },

        "work_years": {
            "bello": "work_year",
            "yf": "jobYear", },

        "expected_positions": {
            "bello": "",
            "yf": "", },

        "expected_industry": {
            "bello": "",
            "yf": "expectIndustry", },

        "expected_work_type": {
            "bello": "",
            "yf": "expectWorkType", },

        "work_start_time": {
            "bello": "work_start_time",
            "yf": "", },

        "job_state": {
            "bello": "",
            "yf": "jobState", },

        "birthday": {
            "bello": "",
            "yf": "birthYear", },

        "integrity": {
            "bello": "resume_integrity",
            "yf": "", },

        "marital_status": {
            "bello": "marital_status",
            "yf": "maritalStatus", },

        "self_description": {
            "bello": "cont_my_desc",
            "yf": "selfEvaluate", },
        }

contact_map = {
        "qq": {
            "bello": "",
            "yf": "", },

        "weibo": {
            "bello": "",
            "yf": "", },

        "github": {
            "bello": "",
            "yf": "", },

        "linkedin": {
            "bello": "",
            "yf": "", },

        "facebook": {
            "bello": "",
            "yf": "", },

        "instagram": {
            "bello": "",
            "yf": "", },

        "blog": {
            "bello": "",
            "yf": "", },
    }

other_map = {

        "category": {
            "bello": "",
            "yf": "", },

        "resume_create_time": {
            "bello": "",
            "yf": "", },

        "resume_update_time": {
            "bello": "",
            "yf": "lastTime", },

        "channel": {
            "bello": "",
            "yf": "", },

        "external_id": {
            "bello": "",
            "yf": "userID", },

        "from_url": {
            "bello": "",
            "yf": "", },

        "evauation": {
            "bello": "",
            "yf": "", },

        "extra_info": {
            "bello": "",
            "yf": "", },

        "path": {
            "bello": "",
            "yf": "", },
    }


section_map = {

        "skills": {
            "bello": "skills_objs",
            "yf": "skillList",
            },

        "educations": {
            "bello": "education_objs",
            "yf": "eduList",
            },

        "experiences": {
            "bello": "job_exp_objs",
            "yf": "workList",
            },

        "projects": {
            "bello": "proj_exp_objs",
            "yf": "projectList",
            },

        "trainings": {
            "bello": "training_objs",
            "yf": "trainList",
            },

        "certificates": {
            "bello": "cert_objs",
            "yf": "",
            },

        "languages": {
            "bello": "lang_objs",
            "yf": "languagesList",
            },
}


section_detail_map = {

        "skills": {
            "name": {"bello": "skills_name", "yf": "skillName"},
            "level": {"bello": "", "yf": "masterDegree"},
            "description": {"bello": "", "yf": ""},
            },

        "educations": {
            "college": {"bello": "edu_college", "yf": "schoolName"},
            "major": {"bello": "edu_major", "yf": "specialty"},
            "start_date": {"bello": "start_date", "yf": "startTime"},
            "end_date": {"bello": "end_date", "yf": "endTime"},
            "degree": {"bello": "edu_degree_norm", "yf": ""},
            "college_type": {"bello": "edu_college_type", "yf": ""},
            "recruit": {"bello": "edu_recruit", "yf": ""},
            "tags": {"bello": "", "yf": ""},
            "is_985": {"bello": "", "yf": ""},
            "is_211": {"bello": "", "yf": ""},
            },

        "experiences": {
            "salary": {"bello": "", "yf": "salary"},
            "department": {"bello": "job_dept", "yf": ""},
            "start_date": {"bello": "start_date", "yf": "startTime"},
            "end_date": {"bello": "end_date", "yf": "endTime"},
            "position": {"bello": "job_positaion", "yf": "jobTitle"},
            "location": {"bello": "", "yf": ""},
            "content": {"bello": "job_content", "yf": "workDesc"},
            "company": {"bello": "job_cpy", "yf": "compName"},
            "company_nature": {"bello": "", "yf": "compProperty"},
            "comp_size": {"bello": "", "yf": "compSize"},
            "comp_industry": {"bello": "", "yf": "compIndustry"},
            "duration": {"bello": "job_duration", "yf": ""},
            "report_to": {"bello": "", "yf": ""},
            "staff": {"bello": "", "yf": ""},
            },

        "projects": {
            "name": {"bello": "proj_name", "yf": "projectName"},
            "position": {"bello": "", "yf": ""},
            "start_date": {"bello": "start_date", "yf": "startTime"},
            "end_date": {"bello": "end_date", "yf": "endTime"},
            "responsibility": {"bello": "proj_resp", "yf": "responsibilityDesc"},
            "content": {"bello": "proj_content", "yf": "projectDesc"},
            "tools": {"bello": "", "yf": "dev_tools"},
            "software": {"bello": "", "yf": "software"},
            },

        "trainings": {
            "start_date": {"bello": "start_date", "yf": "startTime"},
            "end_date": {"bello": "end_date", "yf": "endTime"},
            "content": {"bello": "train_cont", "yf": ""},
            "name": {"bello": "", "yf": "trainName"},
            "machinery": {"bello": "", "yf": "machinery"},
            "certificate_name": {"bello": "", "yf": "certificateName"},
            "address": {"bello": "", "yf": "address"},
            },

        "languages": {
            "language_name": {"bello": "language_name", "yf": "languageName"},
            "language_read_write": {"bello": "language_read_write", "yf": "readWriteSkill"},
            "language_listen_speak": {"bello": "language_listen_speak", "yf": "hearSpeakSkill"},
            },
    }
