const crypto = require('crypto');
/**
 * Description : WhatsApp Webhook Parser Main Class
 * @param {any} webhookData
 * @returns {any}
 */
class WhatsAppWebhookParser {
      constructor(webhookData, cassandraClient) {
        this.webhookData = webhookData;
        this.client = cassandraClient; // Optional Cassandra client
      }
      
      createHash(senderPhone, receiverPhone) {
        // Sort data
        const data = [senderPhone, receiverPhone].sort();
      
        // Concatenate
        const combinedString = data.join('');
      
        // Hash using SHA-256
        const hash = crypto.createHash('sha256').update(combinedString).digest('hex');
        return hash;
      }

        /** 
         *  @fdciabdul
         *  Get Type of text Message
         * @returns {any}
         */
        getMessage() {
          try {
            if (
              this.webhookData.object === 'whatsapp_business_account' &&
              this.webhookData.entry
            ) {
              const entry = this.webhookData.entry[0];
              let forward;
        
              if (entry.changes) {
                const change = entry.changes[0];
                const value = change.value;
        
                if (
                  value.messaging_product === 'whatsapp' &&
                  value.messages &&
                  value.messages[0].type === 'text'
                ) {
                  const SetType = entry.changes[0].value.messages[0];
                  if (SetType.context && SetType.context.forwarded !== undefined) {
                    forward = SetType.context.forwarded;
                  }
                  return {
                    type: 'text',
                    from: value.messages[0].from,
                    text: value.messages[0].text.body,
                    timestamp: value.messages[0].timestamp,
                    senderName: value.contacts[0].profile.name,
                    wa_id: value.messages[0].id,
                    timestamp: value.messages[0].timestamp,
                    forwarded: SetType.context && SetType.context.forwarded ? true : false,
                    quoted: SetType.context && SetType.context.forwarded ? false : SetType.context ? SetType.context : false,
                  };
                }
              }
            }
          } catch (error) {
             Console.log(error);
          }
          return null;
        }
      
      
        /**
         * Get Type of text Message with Security Notification
         * @returns {any}
         */
        getMessageWithSecurityNotification() {
          if (
            this.webhookData.object === 'whatsapp_business_account' &&
            this.webhookData.entry
          ) {
            const entry = this.webhookData.entry[0];
      
            if (entry.changes) {
              const change = entry.changes[0];
              const value = change.value;
      
              if (
                value.messaging_product === 'whatsapp' &&
                value.messages &&
                value.messages[0].type === 'text' &&
                value.messages[0].identity
              ) {
                return {
                  type: 'text_with_security_notification',
                  from: value.messages[0].from,
                  text: value.messages[0].text.body,
                  timestamp: value.messages[0].timestamp,
                  senderName: value.contacts[0].profile.name,
                  wa_id: value.messages[0].id,
                  identity: {
                    acknowledged: value.messages[0].identity.acknowledged,
                    created_timestamp: value.messages[0].identity.created_timestamp,
                    hash: value.messages[0].identity.hash,
                  },
                };
              }
            }
          }
      
          return null;
        }
      
        /**
         * Get Type Of Reaction Message
         * @returns {any}
         */
        getMessageWithReaction() {
          if (
            this.webhookData.object === 'whatsapp_business_account' &&
            this.webhookData.entry
          ) {
            const entry = this.webhookData.entry[0];
      
            if (entry.changes) {
              const change = entry.changes[0];
              const value = change.value;
      
              if (
                value.messaging_product === 'whatsapp' &&
                value.messages &&
                value.messages[0].type === 'reaction'
              ) {
                return {
                  type: 'reaction',
                  from: value.messages[0].from,
                  timestamp: value.messages[0].timestamp,
                  senderName: value.contacts[0].profile.name,
                  wa_id: value.messages[0].id,
                  reaction: {
                    emoji: value.messages[0].reaction.emoji,
                    message_id: value.messages[0].reaction.messsage_id,
                  },
                };
              }
            }
          }
      
          return null;
        }
      
        /**
         * Get type of Image Message
         * @returns {any}
         */
        getImageMessage() {
          if (
            this.webhookData.object === 'whatsapp_business_account' &&
            this.webhookData.entry
          ) {
            const entry = this.webhookData.entry[0];
      
            if (entry.changes) {
              const change = entry.changes[0];
              const value = change.value;
      
              if (
                value.messaging_product === 'whatsapp' &&
                value.messages &&
                value.messages[0].type === 'image'
              ) {
                const SetType = entry.changes[0].value.messages[0];
                const forwarded = SetType.context && SetType.context.forwarded ? true : false;
                return {
                  type: 'image',
                  from: value.messages[0].from,
                  timestamp: value.messages[0].timestamp,
                  senderName: value.contacts[0].profile.name,
                  wa_id: value.messages[0].id,
                  forwarded: forwarded,
                  image: {
                    caption: value.messages[0].image.caption,
                    mimeType: value.messages[0].image.mime_type,
                    sha256: value.messages[0].image.sha256,
                    id: value.messages[0].image.id,
                  },
                };
              }
            }
          }
      
          return null;
        }
      
      
        /**
         * Get Type of Sticker Message
         * @returns {any}
         */
        getStickerMessage() {
          if (
            this.webhookData.object === 'whatsapp_business_account' &&
            this.webhookData.entry
          ) {
            const entry = this.webhookData.entry[0];
      
            if (entry.changes) {
              const change = entry.changes[0];
              const value = change.value;
      
              if (
                value.messaging_product === 'whatsapp' &&
                value.messages &&
                value.messages[0].type === 'sticker'
              ) {
                return {
                  type: 'sticker',
                  from: value.messages[0].from,
                  timestamp: value.messages[0].timestamp,
                  senderName: value.contacts[0].profile.name,
                  wa_id: value.messages[0].id,
                  sticker: {
                    id: value.messages[0].sticker.id,
                    animated: value.messages[0].sticker.animated,
                    mimeType: value.messages[0].sticker.mime_type,
                    sha256: value.messages[0].sticker.sha256,
                  },
                };
              }
            }
          }
      
          return null;
        }
      
        /**
         * Get Contact Message
         * @returns {any}
         */
        getContactMessage() {
          if (
            this.webhookData.object === 'whatsapp_business_account' &&
            this.webhookData.entry
          ) {
            const entry = this.webhookData.entry[0];
      
            if (entry.changes) {
              const change = entry.changes[0];
              const value = change.value;
      
              if (
                value.messaging_product === 'whatsapp' &&
                value.messages &&
                value.messages[0].contacts
              ) {
                return {
                  type: 'contact',
                  from: value.messages[0].from,
                  timestamp: value.messages[0].timestamp,
                  senderName: value.contacts[0].profile.name,
                  wa_id: value.messages[0].id,
                  contact: value.messages[0].contacts[0],
                };
              }
            }
          }
      
          return null;
        }
      
        /**
         * Get Location Message
         * @returns {any}
         */
        getLocationMessage() {
          const message = this.webhookData.entry[0].changes[0].value.messages[0];
          const location = message.location;
          return {
            type: 'location',
            from: message.from,
            id: message.id,
            timestamp: message.timestamp,
            location: {
              latitude: location.latitude,
              longitude: location.longitude,
              name: location.name,
              address: location.address,
            },
          };
        }
    
    /*
    *  Write to cassandra. 
    */
        async parseAndWriteToCassandra() {
          if (!this.webhookData || !this.client) {
            console.warn("Missing required data. Message not processed.");
            return;
          }
      
          // Parse the webhook data
          const message = this.parseMessage();
      
          if (!message) {
            return;
          }
      
          // Assuming you have conversationId in the message
          const messageText = message.text;
          const receiverPhone = message.display_number;
          const senderPhone = message.from;
          const conversationId = this.createHash(senderPhone, receiverPhone); 
      
          const query = `INSERT INTO conversations.convervations (conversation_id, message_id, messagetext, receiverphone, senderphone) VALUES (?, now(), ?, ?, ?)`;
      
          try {
            await this.client.execute(query, [conversationId, messageText, receiverPhone, senderPhone]);
            console.log("Message written to Cassandra:", message);
            return message;
          } catch (error) {
            console.error("Error writing message to Cassandra:", error);
          }
        }  

    /**
     * Main Function to parse message
     * @param {any} webhookData
     * @param {any} webhookEvent
     * @param {any} webhookEventName
     * @param {any} webhookEventTime
     * @param {any} webhookEventSource
     * @returns {any}
     */
    parseMessage() {
        if (
            this.webhookData.object === 'whatsapp_business_account' &&
            this.webhookData.entry
        ) {
            const entry = this.webhookData.entry[0];

            if (entry.changes) {
                const change = entry.changes[0];
                const value = change.value;

                // Extract phone number details
                const phone_id = value.metadata.phone_number_id;
                const display_number = value.metadata.display_phone_number;

                if (value.messaging_product === 'whatsapp' && value.messages) {
                    const message = value.messages[0];
                    const messageType = message.type;

                    if (messageType) {
                        switch (messageType) {
                            case 'text':
                                return {
                                    ...this.getMessage(),
                                    phone_id,
                                    display_number,
                                };
                            case 'text_with_security_notification':
                                return {
                                    ...this.getMessageWithSecurityNotification(),
                                    phone_id,
                                    display_number,
                                };
                            case 'reaction':
                                return {
                                    ...this.getMessageWithReaction(),
                                    phone_id,
                                    display_number,
                                };
                            case 'image':
                                return {
                                    ...this.getImageMessage(),
                                    phone_id,
                                    display_number,
                                };
                            case 'sticker':
                                return {
                                    ...this.getStickerMessage(),
                                    phone_id,
                                    display_number,
                                };
                            default:
                                return null;
                        }
                    } else if (message.contacts) {
                        return {
                            ...this.getContactMessage(),
                            phone_id,
                            display_number,
                        };
                    } else if (message.location) {
                        return {
                            ...this.getLocationMessage(),
                            phone_id,
                            display_number,
                        };
                    }
                }
            }
        }else{

        if (this.webhookData.entry) {
            const entry = this.webhookData.entry[0];

            if (entry.changes) {
                    const change = entry.changes[0];
                    const value = change.value;

                    if (
                            value.messaging_product === "whatsapp" &&
                            value.statuses &&
                            value.statuses[0]
                    ) {
                            const status = value.statuses[0];
                            return {
                                    type: 'status',
                                    id: status.id,
                                    status: status.status,
                                    timestamp: status.timestamp,
                                    recipient_id: status.recipient_id,
                            };
                    }
            }
         }
        }

        return;
    }
}

module.exports =  WhatsAppWebhookParser;