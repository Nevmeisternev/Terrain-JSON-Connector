//  Copyright notice
//
//  (c) 2021 Newell Richards <newell@richards.id.au>
//
//  All rights reserved
//
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
//  This copyright notice MUST APPEAR in all copies of the script!

//Global Community Connector
var debug = false;

/**
 * Throws and logs script exceptions.
 *
 * @param {String} message The exception message
 */
function sendUserError(message, debugMsg) {
  var cc = DataStudioApp.createCommunityConnector();
  if (debug) {
    Logger.log('Error: ' + message + ', debug: ' + debugMsg);
    cc.newUserError()
    .setText(message)
    .setDebugText(debugMsg)
    .throwException();
  }
  else {
    Logger.log('Error: ' + message);
    cc.newUserError()
    .setText(message)
    .throwException();
  }
}

/**
 * Returns the Auth Type of this connector.
 * @return {object} The Auth type.
 */
function getAuthType() {
  var cc = DataStudioApp.createCommunityConnector();
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.KEY)
    .setHelpUrl('https://www.example.org/connector-auth-help')
    .build();
}

/**
 * Returns true if the auth service has access.
 * @return {boolean} True if the auth service has access.
 */
function isAuthValid() {
  var userProperties = PropertiesService.getUserProperties();
  var key = userProperties.getProperty('dscc.key');
  // This assumes you have a validateKey function that can validate
  // if the key is valid.
  return validateKey(key);
}

/**
 * Resets the auth service.
 */
function resetAuth() {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty('dscc.key');
}

/**
 * Validates the key.
 */
function validateKey(key) {
  return true;
}

/**
 * function  `isAdminUser()`
 *
 * @returns {Boolean} Currently just returns false. Should return true if the current authenticated user at the time
 *                    of function execution is an admin user of the connector.
 */
function isAdminUser() {
  return debug;
}

/**
 * Returns an array with the id and the index.
 */
function getHeaderOrder(header_row) {
  var headerindex = [];
  for (var i = 0; i < header_row.length; i++) {
    headerindex[createIdfromName(header_row[i])] = i;
  }
  return headerindex;
}

/**
 * Returns the user configurable options for the connector.
 *
 * Required function for Community Connector.
 *
 * @param   {Object} request  Config request parameters.
 * @returns {Object}          Connector configuration to be displayed to the user.
 */
function getConfig(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();

  config
    .newInfo()
    .setId('instructions')
    .setText('Fill out the form to connect to a JSON data source.');

  config
    .newTextInput()
    .setId('url')
    .setName('Enter the URL of a JSON data source')
    .setHelpText('e.g. https://my-url.org/json')
    .setPlaceholder('https://my-url.org/json');

  config.setDateRangeRequired(false);

  return config.build();
}

/**
 * Gets cached response. If the response has not been cached, make
 * the fetchJSON call, then cache and return the response.
 *
 * @param   {string} url  The URL to get the data from
 * @returns {Object}      The response object
 */
function getCachedData(url) {
  var cacheExpTime = 600;
  var cache = CacheService.getUserCache();
  var cacheKey = url.replace(/[^a-zA-Z0-9]+/g, '');
  var content = [];

  Logger.log('Getting the data.');

  cacheData = cache.get(cacheKey);
  if (cacheData !== null) {
    Logger.log('Getting the data from cache.');
    content = JSON.parse(cacheData);
  } else {
    Logger.log('Getting the data from url.');
    content = ImportJSON(url);
    cache.put(cacheKey, JSON.stringify(content), cacheExpTime);
  }
  return content;
}

/**
* Returns the fields for the connector.
*
* @returns {Object} fields for connector.
*/
function getColumns(content, fields) {

  var headersOrder = getHeaderOrder(content[0]);
  var contentNoHeader = content.slice(1); 
 
  return contentNoHeader.map(function(row) {
    var rowValues = [];
    fields.asArray().forEach(function(field) {
      var id = field.getId();
      var fieldValue = row === null ? '' : row[headersOrder[id]];
      rowValues.push(field, fieldValue);
    });
    return {values: rowValues};
  });
}

/**
* Returns the fields for the connector.
*
* @returns {Object} fields for connector.
*/
function getFields(content) {
  var headers = new Array();
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  headers = content[0];
  for (var i = 0; i < headers.length; i++) {
    fields
    .newDimension()
    .setId(createIdfromName(headers[i]))
    .setName(headers[i])
    .setType(types.TEXT);
  }

  return fields;
}

/**
 * Returns the schema for the given request.
 *
 * @param   {Object} request Schema request parameters.
 * @returns {Object} Schema for the given request.
 */
function getSchema(request) {
  var content = getCachedData(request.configParams.url);
  return {schema: getFields(content).build()};
}

/**
 * Returns the tabular data for the given request.
 *
 * @param   {Object} request  Data request parameters.
 * @returns {Object}          Contains the schema and data for the given request.
 */
function getData(request) {
  var content = getCachedData(request.configParams.url);
  var fields = getFields(request, content);
  var columns = getColumns(content, fields);
    
  return {
    schema: fields.build(),
    rows: columns
  };
}

/*
* 
*/
function createIdfromName(name) {
  return name.replace(' ', '_');
}

/**
* A test to test the connector in development
**/
function test() {
  var test = getCachedData('https://drive.google.com/uc?export=download&id=18co5qooVbmtYKWwSZY9u-zVSQMpIOfUz');
  var fields = getFields(test);
  var columns = getColumns(test, fields);

  Logger.log('Finished the test');
}
