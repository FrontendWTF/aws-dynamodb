angular.module('aws.dynamodb', [
  'aws'
]).factory('DynamoDB', function (
  $q,
  $window,
  AWS
) {
  function DynamoDb (config) {
    var self = this;
    var methods = ['getItem', 'putItem', 'updateItem',
                   'query', 'scan',
                   'batchGetItem', 'batchWriteItem'];
    var dynamoDb = new AWS.DynamoDB(config);
    var db = new $window.DynamoDB(dynamoDb);

    this.config = db.config;
    this.methods = methods;
    methods.map(function (method, index) {
      self[method] = function (params) {
        return callbackToPromise(method, params);
      };
    });

    /**
     * Convert DynamoDB's callback into Promise
     * @param {String} method - request method
     * @param {Object} params - request parameters
     * @returns {Promise}
     */
    function callbackToPromise (method, params) {
      var deferred = $q.defer();

      ['Expected', 'KeyConditions', 'QueryFilter', 'ScanFilter'].map(function (param) {
        if (params[param]) {
          var conditions = [];
          params[param].map(function (condition) {
            conditions.push($window.DynamoDBCondition.apply(this, condition));
          });
          params[param] = conditions;
        }
      });

      db[method](params, function (error, data) {
        if (error) {
          deferred.reject(error);
        } else {
          if (data.Item) {
            data.result = data.Item;
          } else if (data.Items) {
            data.result = data.Items;
          } else if (data.Attributes) {
            data.result = data.Attributes;
          }

          deferred.resolve(data);
        }
      });

      return deferred.promise;
    }
  }

  return DynamoDb;
});
