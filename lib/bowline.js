/**
 * Wrapper for the mqtt library.
 * @module
 */
'use strict'

const mqtt = require('mqtt')

const logger = require('logbook')
const log = logger.createLogger(__filename)

/**
 * Called when a message is received on the topic filter given when calling {@link module:bowline~MQTTWrapper#registerHandler}.
 * @callback messageHandler
 * @param {object} message The message that was received.
 * @param {string} topic The topic the message was received on.
 */

/**
 * Wraps a {@link module:bowline~messageHandler}, filtering out calls with
 * topics not matching the topic filter given when calling {@link module:bowline~MQTTWrapper#registerHandler}.
 * topics not matching the topic filter given when calling {@link module:bowline~MQTTWrapper#registerHandler}.
 * @callback messageHandlerWrapper
 * @param {object} message The message that was received.
 * @param {string} topic The topic the message was received on.
 */

/**
 * Wraps the mqtt library and provides async/await compatible method signatures,
 * as well as better message handling capabilities.
 */
class MQTTWrapper {
  /**
   * Create an MQTT wrapper instance.
   */
  constructor () {
    this.connected = false

    this.topicHandlers = new Set()
  }

  /**
   * Connect to the given server, optionally passing the given options to the
   * mqtt library.
   * @async
   * @param  {string} server Server URL to connect to.
   * @param  {object} options Additional options to be passed to the {@link https://www.npmjs.com/package/mqtt#connect|mqtt library}
   * @return {Promise} Resolves when the connection was established.
   */
  connect (server, options) {
    return new Promise((resolve, reject) => {
      /**
       * mqtt Client instance.
       * @member
       * @internal
       */
      this.client = mqtt.connect(server, options)

      this.client.on('connect', () => {
        log.info('MQTT connected to server', { server, options })
        this.connected = true
        resolve()
      })

      this.client.once('error', err => {
        log.error('Failed to connect to server', { server, err })
        reject(err)
      })

      this.client.on('offline', () => {
        log.info('MQTT client disconnected from server', {server})
        this.connected = false
      })

      /**
       * Resolves when the client goes offline.
       * @member {Promise}
       */
      this.onOffline = new Promise(resolve => {
        this.client.once('offline', resolve)
      })

      this.client.on('message', (topic, message) => {
        logger.correlate(() => {
          log.debug('Received MQTT message', { topic, payload: message })
          try {
            const parsed = message.length === 0 ? null : JSON.parse(message)
            this.topicHandlers.forEach(handler => handler(parsed, topic))
          } catch (err) {
            log.warn('MQTT message is not valid JSON', { payload: message })
          }
        })
      })
    })
  }

  /**
   * Disconnect from the server.
   * @async
   * @param  {Boolean} [force=false] Whether to force disconnecting, discard all queued messages.
   * @return {Promise} Resolves when the client disconnected.
   */
  disconnect (force = false) {
    log.info('Disconnecting from MQTT server', { force })
    return new Promise((resolve, reject) => {
      if (this.client) {
        this.client.end(force, resolve)
      }
    })
  }

  /**
   * Register a handler for the given topic filter.
   * @param  {string} filter The MQTT topic filter to register the handler for.
   * @param  {module:bowline~messageHandler} messageHandler Callback called when
   * a message is received matching the given topic filter.
   * @return {module:bowline~messageHandlerWrapper} A wrapper function around
   * the message handler that can be passed to {@link
   * module:bowline~MQTTWrapper#unregisterHandler} to later remove this handler.
   */
  registerHandler (filter, messageHandler) {
    // Convert MQTT topic filter to a regex by replacing wildcards
    const filterRegex = new RegExp(`^${filter.replace('#', '.*?').replace('+', '[^/]*')}$`)
    log.verbose('Registering handler for topic filter', { filter })
    const handler = (message, topic) => {
      if (topic.match(filterRegex)) {
        log.debug('Dispatching message to message handler', { topic })
        messageHandler(message, topic)
      }
    }
    this.topicHandlers.add(handler)
    return handler
  }

  /**
   * Unregister a handler previously registered through {@link
   * module:bowline~MQTTWrapper#registerHandler}.
   * @param {module:bowline~messageHandlerWrapper} messageHandler The handler
   * returned by {@link module:bowline~MQTTWrapper#registerHandler} to be
   * unregistered.
   */
  unregisterHandler (messageHandler) {
    log.verbose('Removing message handler')
    this.topicHandlers.delete(messageHandler)
  }

  /**
   * Throw when the client is not connected, otherwise do nothing.
   * @private
   */
  _checkConnected () {
    if (!this.connected) throw new Error('Not connected to MQTT server')
  }

  /**
   * Subscribe to the given topic filter, optionally registering the given
   * message handler.
   * @async
   * @param  {string} filter The MQTT topic filter to subscribe to.
   * @param  {module:bowline~messageHandler} messageHandler The
   * message handler to register with the given filter.
   * @return {Promise<module:bowline~messageHandlerWrapper?>} Resolves
   * when the subscription was successfull, returning the registered wrapper
   * function as documented in {@link
   * module:bowline~MQTTWrapper#registerHandler} if a message handler
   * was given.
   */
  subscribe (filter, messageHandler) {
    this._checkConnected()
    return new Promise((resolve, reject) => {
      log.verbose('Subscribing to filter', { filter })
      this.client.subscribe(filter, { qos: 0 }, (err, granted) => {
        if (err) {
          log.verbose('Failed to subscribe to filter', { filter })
          return reject(err)
        }
        log.verbose('Subscribed to filter', { filter })
        resolve(messageHandler && this.registerHandler(filter, messageHandler))
      })
    })
  }

  /**
   * Publish a message on a topic.
   * @async
   * @param  {string} topic The topic to publish the message on.
   * @param  {any} message The message to publish.
   * @param  {Object} [options] Additional options for publishing the message.
   * @param  {boolean} options.retain Set the retain flag when publishing th message.
   * @param  {number} options.qos The qos to use when publishing the message.
   * @param  {boolean} options.dup Mark the message as a duplicate.
   * @return {Promise} Resolves when the message was published.
   */
  publish (topic, message, options = { retain: false, qos: 0, dup: false }) {
    this._checkConnected()
    return new Promise((resolve, reject) => {
      log.verbose('Publishing message', { topic, message, options })
      const payload = message === null
        ? message
        : JSON.stringify(message)
      this.client.publish(topic, payload, options, err => {
        if (err) {
          log.verbose('Failed to publish message', { topic, err })
          return reject(err)
        }
        log.verbose('Published message', { topic })
        resolve()
      })
    })
  }
}

module.exports = {
  MQTTWrapper
}
