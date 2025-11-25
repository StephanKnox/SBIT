date_loader_cols_mapping = {
                        "date": {
                            "col_type": "date",
                            "json_path": "date"
                        },
                        "dayofmonth": {
                            "col_type": "int",
                            "json_path": "dayofmonth"
                        },
                        "dayofweek": {
                            "col_type": "int",
                            "json_path": "dayofweek"
                        },
                        "dayofyear": {
                            "col_type": "int",
                            "json_path": "dayofyear"
                        },
                        "month": {
                            "col_type": "int",
                            "json_path": "month"
                        },
                        "week": {
                            "col_type": "int",
                            "json_path": "week"
                        },
                        "year": {
                            "col_type": "int",
                            "json_path": "year"
                        },
                         "week_part": {
                            "col_type": "string",
                            "json_path": "week_part"
                        }
                    }

user_profiles_cols_mapping = {
                        "timestamp": {
                            "col_type": "long",
                            "json_path": "timestamp"
                        },    
                        "user_id": {
                            "col_type": "long",
                            "json_path": "parsedJson.user_id"
                        },
                        "dob": {
                            "col_type": "string",
                            "json_path": "parsedJson.dob"
                        },
                        "sex": {
                            "col_type": "string",
                            "json_path": "parsedJson.sex"
                        },
                        "gender": {
                            "col_type": "string",
                            "json_path": "parsedJson.gender"
                        },

                        "first_name": {
                            "col_type": "string",
                            "json_path": "parsedJson.first_name"
                        },

                        "last_name": {
                            "col_type": "string",
                            "json_path": "parsedJson.last_name"
                        },

                        "street_address": {
                            "col_type": "string",
                            "json_path": "parsedJson.address.street_address"
                        },

                        "city": {
                            "col_type": "string",
                            "json_path": "parsedJson.address.city"
                        },
                        "state": {
                            "col_type": "string",
                            "json_path": "parsedJson.address.state"
                        },
                        "zip": {
                            "col_type": "int",
                            "json_path": "parsedJson.address.zip"
                        }
                    }

workouts_cols_mapping = {
                        "user_id": {
                            "col_type": "long",
                            "json_path": "parsedJson.user_id"
                        },
                        "workout_id": {
                            "col_type": "long",
                            "json_path": "parsedJson.workout_id"
                        },
                        "timestamp": {
                            "col_type": "timestamp",
                            "json_path": "parsedJson.timestamp"
                        },
                        "action": {
                            "col_type": "string",
                            "json_path": "parsedJson.action"
                        },

                        "session_id": {
                            "col_type": "long",
                            "json_path": "parsedJson.session_id"
                        }
}

bpm_cols_mapping = {
                        "device_id": {
                            "col_type": "long",
                            "json_path": "parsedJson.device_id"
                        },
                        "time": {
                            "col_type": "timestamp",
                            "json_path": "parsedJson.time"
                        },
                        "heartrate": {
                            "col_type": "double",
                            "json_path": "parsedJson.heartrate"
                        }
}