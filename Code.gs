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
var debug = true;

/**
 * Throws and logs script exceptions.
 *
 * @param {String} message The exception message
 */
function sendUserError(message, debugMsg) {
  var cc = DataStudioApp.createCommunityConnector();
  if (debug) {
    cc.newUserError()
    .setText(message)
    .setDebugText(debugMsg)
    .throwException();
  }
  else {
    cc.newUserError()
    .setText(message)
    .throwException();
  }
}

/**
 * function  `getAuthType()`
 *
 * @returns {Object} `AuthType` used by the connector.
 */
function getAuthType() {
  return {type: 'NONE'};
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

  cacheData = cache.get(cacheKey);
  if (cacheData !== null) {
    content = JSON.parse(cacheData);
  } else {
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
function getFields(content) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;
  
  var header_row = content[0];
  
  header_row.forEach(function(header) {
  fields
    .newDimension()
    .setId(header.replace(' ', '_'))
    .setName(header)
    .setType(types.TEXT);
  });

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

function getData(request) {
  var content = getCachedData(request.configParams.url);
  var fields = getFields(request, content);
  var columns = getColumns(content, fields);
    
  return {
    schema: fields.build(),
    rows: columns
  };
}
 */

/**
* A test to test the connector in development
**/
function test() {
  var fields = getFields(test);
  Logger.log(test);
}
