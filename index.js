const common = require("oci-common");
const st = require("oci-streaming");
const crypto = require("crypto");

const configurationFilePath = "~/.oci/config";
const configProfile = "DEFAULT";

const provider = new common.ConfigFileAuthenticationDetailsProvider(
  configurationFilePath,
  configProfile
);


const compartmentId = "ocid1.compartment.oc1..aaaaaaaaibjn7uy24pzpt3hqvq4owmzsq3ozllxcho5vjujlq3e52u6ojtuq"
const adminClient = new st.StreamAdminClient({ authenticationDetailsProvider: provider });
const client = new st.StreamClient({ authenticationDetailsProvider: provider });


(async () => {

  //Get stream ID
  const stream = await getStream(compartmentId)
  console.log("O ID é: ", stream.id)
  console.log("O endpoint é: ", stream.messagesEndpoint)
  client.endpoint = stream.messagesEndpoint

  //Put new message
  await publishExampleMessages(client, stream.id, "Olá mundo")


  async function getStream(compartmentId) {
    const listStreamsRequest = {
      compartmentId: compartmentId,
      name: 'NewStreaming',
      lifecycleState: st.models.Stream.LifecycleState.Active.toString()
    };
    const listStreamsResponse = await adminClient.listStreams(listStreamsRequest);
    const stream = listStreamsResponse.items[0]
    return stream
  }

  async function publishExampleMessages(client, streamId, text) {
    // build up a putRequest and publish some messages to the stream
    const id = crypto.randomBytes(20).toString('hex');
    let messages = [];
    for (let i = 1; i <= 1; i++) {
      let entry = {
        key: Buffer.from(id).toString("base64"),
        value: Buffer.from(text).toString("base64")
      };
      messages.push(entry);
    }
  
    console.log("Publishing %s messages to stream %s.", messages.length, streamId);
    const putMessageDetails = { messages: messages };
    const putMessagesRequest = {
      putMessagesDetails: putMessageDetails,
      streamId: streamId
    };
    const putMessageResponse = await client.putMessages(putMessagesRequest);
    for (var entry of putMessageResponse.putMessagesResult.entries)
      console.log("Published messages to parition %s, offset %s", entry.partition, entry.offset);
  }

})();


