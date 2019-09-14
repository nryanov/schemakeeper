const SchemaKeeperApi = (function (baseUrl) {
    function jsonRPC(url, method, paramObject, callback) {
        console.log("URL: ", url, " Method: ", method, " Params: ", JSON.stringify(paramObject));
        return $.ajax(url, {
                data: JSON.stringify(paramObject),
                method: method,
                contentType: "application/json; charset=utf-8",
                dataType: "json",
                success: callback,
                error: function(msg) {
                    alert(msg);
                }
            }
        )
    }

    SchemaKeeperApi.prototype.subjects = function (callback) {
        jsonRPC(baseUrl + "/v1/subjects", "GET", {}, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.getSubjectMeta = function (subject, callback) {
        jsonRPC(baseUrl + "/v1/subjects/" + subject, "GET", {}, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.schemaById = function (id, callback) {
        jsonRPC(baseUrl + "/v1/schema/" + id, "GET", {}, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.subjectVersions = function (subject, callback) {
        jsonRPC(baseUrl + "/v1/subjects/" + subject + "/versions", "GET", {}, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.checkSubjectSchemaCompatibility = function (subject, schema, callback) {
        jsonRPC(baseUrl + "/v1/compatibility/" + subject, "GET", {
            "schema": schema
        }, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.deleteSubjectVersion = function (subject, version, callback) {
        jsonRPC(baseUrl + "/v1/subjects/" + subject + "/versions/" + version, "DELETE", {}, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.deleteSubject = function (subject, callback) {
        jsonRPC(baseUrl + "/v1/subjects/" + subject, "DELETE", {}, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.subjectSchemaByVersion = function (subject, version, callback) {
        jsonRPC(baseUrl + "/v1/subjects/" + subject + "/versions/" + version, "GET", {}, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.subjectOnlySchemaByVersion = function (subject, version, callback) {
        jsonRPC(baseUrl + "/v1/subjects/" + subject + "/versions/" + version + "/schema", "GET", {}, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.schema = function (id, callback) {
        jsonRPC(baseUrl + "/v1/schema/" + id, "GET", {}, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.registerNewSubjectSchema = function (subject, schema, callback) {
        jsonRPC(baseUrl + "/v1/subjects/versions/" + subject, "POST", {
            "schema": schema,
            "schemaType": "avro"
        }, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.registerNewSubject = function (subject, schema, compatibilityType, callback) {
        jsonRPC(baseUrl + "/v1/subjects/" + subject, "POST", {
            "schema": schema,
            "schemaType": "avro",
            "compatibilityType": compatibilityType
        }, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.getSubjectCompatibilityConfig = function (subject, callback) {
        jsonRPC(baseUrl + "/v1/compatibility/" + subject, "GET", {}, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.updateSubjectCompatibilityConfig = function (subject, compatibilityType, callback) {
        jsonRPC(baseUrl + "/v1/compatibility/" + subject, "PUT", {
            "compatibilityType": compatibilityType
        }, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.getGlobalCompatibilityConfig = function (callback) {
        jsonRPC(baseUrl + "/v1/compatibility", "GET", {}, function (response) {
            callback(response);
        })
    };

    SchemaKeeperApi.prototype.updateGlobalCompatibilityConfig = function (compatibilityType, callback) {
        jsonRPC(baseUrl + "/v1/compatibility", "PUT", {
            "compatibilityType": compatibilityType
        }, function (response) {
            callback(response);
        })
    };
});
