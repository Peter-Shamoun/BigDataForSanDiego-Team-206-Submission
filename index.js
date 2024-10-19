import Fastify from 'fastify';
import WebSocket from 'ws';
import dotenv from 'dotenv';
import fastifyFormBody from '@fastify/formbody';
import fastifyWs from '@fastify/websocket';

// Load environment variables from .env file
dotenv.config();

// Retrieve the OpenAI API key from environment variables.
const { OPENAI_API_KEY, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER } = process.env;



if (!OPENAI_API_KEY) {
    console.error('Missing OpenAI API key. Please set it in the .env file.');
    process.exit(1);
}

// Initialize Fastify
const fastify = Fastify();
fastify.register(fastifyFormBody);
fastify.register(fastifyWs);

// Constants
const SYSTEM_MESSAGE = 'You are an AI-powered voice assistant for Sharp Healthcare, known as the "Shape Healthcare Assistant." Your primary role is to assist users with their medical needs by offering several key services. You can help users schedule appointments with healthcare providers, provide them with details about their prescription routines, including medication names and timings, and assist in finding the nearest hospital based on their location. Additionally, you are capable of displaying upcoming appointments, helping users cancel appointments when needed, and relaying important messages to caregivers or healthcare providers. Your interaction with the user should always begin with the greeting: "Hello, I am an AI-powered Voice Assistant for Sharp Healthcare. How can I help you today?" It is essential that you deliver timely, clear, and empathetic assistance, ensuring the confidentiality and security of all medical information. Your goal is to prioritize the users healthcare needs and provide accurate and efficient support in all interactions.'

const VOICE = 'alloy';
const PORT = process.env.PORT || 5050; // Allow dynamic port assignment

// List of Event Types to log to the console. See the OpenAI Realtime API Documentation: https://platform.openai.com/docs/api-reference/realtime
const LOG_EVENT_TYPES = [
    'error',
    'response.content.done',
    'rate_limits.updated',
    'response.done',
    'input_audio_buffer.committed',
    'input_audio_buffer.speech_stopped',
    'input_audio_buffer.speech_started',
    'session.created'
];

// Show AI response elapsed timing calculations
const SHOW_TIMING_MATH = false;

// Root Route
fastify.get('/', async (request, reply) => {
    reply.send({ message: 'Twilio Media Stream Server is running!' });
});

// Route for Twilio to handle incoming calls
// <Say> punctuation to improve text-to-speech translation
fastify.all('/incoming-call', async (request, reply) => {
    const twimlResponse = `<?xml version="1.0" encoding="UTF-8"?>
                          <Response>
                              <Connect>
                                  <Stream url="wss://${request.headers.host}/media-stream" />
                              </Connect>
                          </Response>`;

    reply.type('text/xml').send(twimlResponse);
});


// WebSocket route for media-stream
fastify.register(async (fastify) => {
    fastify.get('/media-stream', { websocket: true }, (connection, req) => {
        console.log('Client connected');

        // Connection-specific state
        let streamSid = null;
        let latestMediaTimestamp = 0;
        let lastAssistantItem = null;
        let markQueue = [];
        let responseStartTimestampTwilio = null;

        const openAiWs = new WebSocket('wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01', {
            headers: {
                Authorization: `Bearer ${OPENAI_API_KEY}`,
                "OpenAI-Beta": "realtime=v1"
            }
        });

        // Control initial session with OpenAI
        const initializeSession = () => {
            const sessionUpdate = {
                type: 'session.update',
                session: {
                    turn_detection: {
                         type: 'server_vad',
                        threshold: 0.6,
                        prefix_padding_ms: 500,
                        silence_duration_ms: 2000
                        },
                    input_audio_format: 'g711_ulaw',
                    output_audio_format: 'g711_ulaw',
                    voice: VOICE,
                    instructions: SYSTEM_MESSAGE,
                    modalities: ["text", "audio"],
                    temperature: 0.8,
                }
            };
            tools: [
                {
                    name: 'view_prescription',
                    description: 'View the user\'s current prescription details.',
                    parameters: {
                        type: 'object',
                        properties: {
                            user_id: { type: 'string' }
                        },
                        required: ['user_id']
                    }
                },
                {
                    name: 'schedule_appointments',
                    description: 'Schedule an appointment for the user.',
                    parameters: {
                        type: 'object',
                        properties: {
                            user_id: { type: 'string' },
                            datetime: { type: 'string' },
                            reason: { type: 'string' },
                            doctor: { type: 'string' }
                        },
                        required: ['user_id', 'datetime', 'reason', 'doctor']
                    }
                },
                {
                    name: 'nearest_hospital',
                    description: 'Find the nearest hospital for the user.',
                    parameters: {
                        type: 'object',
                        properties: {
                            user_id: { type: 'string' }
                        },
                        required: ['user_id']
                    }
                },
                {
                    name: 'view_upcoming_app',
                    description: 'View the user\'s upcoming appointments.',
                    parameters: {
                        type: 'object',
                        properties: {
                            user_id: { type: 'string' }
                        },
                        required: ['user_id']
                    }
                },
                {
                    name: 'cancel_app',
                    description: 'Cancel the user\'s appointment.',
                    parameters: {
                        type: 'object',
                        properties: {
                            user_id: { type: 'string' },
                            datetime: { type: 'string' },
                            doctor: { type: 'string' }
                        },
                        required: ['user_id', 'datetime', 'doctor']
                    }
                },
                {
                    name: 'relay_message',
                    description: 'Relay a message to the user\'s doctor.',
                    parameters: {
                        type: 'object',
                        properties: {
                            user_id: { type: 'string' },
                            doctor: { type: 'string' },
                            message: { type: 'string' }
                        },
                        required: ['user_id', 'doctor', 'message']
                    }
                }
            ]
        

            console.log('Sending session update:', JSON.stringify(sessionUpdate));
            openAiWs.send(JSON.stringify(sessionUpdate));

            // Uncomment the following line to have AI speak first:
             sendInitialConversationItem();

             const handleFunctionCall = (functionName, params) => {
                switch (functionName) {
                    /*case 'view_prescriptions':
                        viewPrescriptions(params.user_id);
                        break;*/
                     /*case 'schedule_appointments':
                        scheduleAppointments(params.user_id, params.datetime, params.reason, params.doctor);
                        break;*/
                    case 'nearest_hospital':
                        nearestHospital(params.user_id);
                        break;
                    /*case 'View upcoming appointments':
                        viewUpcomingAppointments(params.user_id);
                        break;
                    case 'cancel_appointment':
                        cancelAppointment(params.user_id, params.datetime, params.doctor);
                        break;
                    case 'relay_message':
                        relayMessage(params.user_id, params.doctor, params.message);
                        break;
                    default:
                        console.error(`Function ${functionName} is not defined.`);*/
                }
            };
        };
        const nearestHospital = (user_id) => {
            console.log('Searching for nearest hospital');
            const hospitals = [
                'Sharp Chula Vista Medical Center',
                'Sharp Coronado Hospital',
                'Sharp Grossmont Hospital',
                'Sharp Memorial Hospital',
                'Sharp Mesa Vista Hospital'
            ];
            const selectedHospital = hospitals[Math.floor(Math.random() * hospitals.length)];

            openAiWs.send(JSON.stringify({
                type: 'conversation.item.create',
                item: {
                    type: 'function_call_output',
                    function_call_output: 'Successfully located hospital'
                }
            }));

            openAiWs.send(JSON.stringify({
                type: 'response.create',
                response: {
                    modalities: ['text'],
                    instructions: `Tell the user that The nearest hospital to them is ${selectedHospital}.`
                }
            }));
        };




            openAiWs.on('message', (data) => {
                try {
                    const message = JSON.parse(data);
                    if (message.type === 'function_call') {
                        const functionName = message.name;
                        const params = message.parameters;
                        handleFunctionCall(functionName, params);
                    }
                } catch (error) {
                    console.error('Error processing message:', error);
                }
            });
            

        // Send initial conversation item if AI talks first
        const sendInitialConversationItem = () => {
            const initialConversationItem = {
                type: 'conversation.item.create',
                item: {
                    type: 'message',
                    role: 'user',
                    content: [
                        {
                            type: 'input_text',
                            text: 'Greet the user by saying: "Hello, I am an AI-powered Voice Assistant for Sharp Healthcare. How can I assist you today?"After greeting, you should focus on understanding the users needs and provide tailored support based on their inquiry. Its crucial to ask clarifying questions if the users request is not fully clear, ensuring you understand their needs. Once you have understood the request, follow through by providing helpful information or taking the necessary actions to complete the request efficiently and accurately'



                        }
                    ]
                }
            };

            if (SHOW_TIMING_MATH) console.log('Sending initial conversation item:', JSON.stringify(initialConversationItem));
            openAiWs.send(JSON.stringify(initialConversationItem));
            openAiWs.send(JSON.stringify({ type: 'response.create' }));
        };

        // Handle interruption when the caller's speech starts
        const handleSpeechStartedEvent = () => {
            if (markQueue.length > 0 && responseStartTimestampTwilio != null) {
                const elapsedTime = latestMediaTimestamp - responseStartTimestampTwilio;
                if (SHOW_TIMING_MATH) console.log(`Calculating elapsed time for truncation: ${latestMediaTimestamp} - ${responseStartTimestampTwilio} = ${elapsedTime}ms`);

                if (lastAssistantItem) {
                    const truncateEvent = {
                        type: 'conversation.item.truncate',
                        item_id: lastAssistantItem,
                        content_index: 0,
                        audio_end_ms: elapsedTime
                    };
                    if (SHOW_TIMING_MATH) console.log('Sending truncation event:', JSON.stringify(truncateEvent));
                    openAiWs.send(JSON.stringify(truncateEvent));
                }

                connection.send(JSON.stringify({
                    event: 'clear',
                    streamSid: streamSid
                }));

                // Reset
                markQueue = [];
                lastAssistantItem = null;
                responseStartTimestampTwilio = null;
            }
        };

        // Send mark messages to Media Streams so we know if and when AI response playback is finished
        const sendMark = (connection, streamSid) => {
            if (streamSid) {
                const markEvent = {
                    event: 'mark',
                    streamSid: streamSid,
                    mark: { name: 'responsePart' }
                };
                connection.send(JSON.stringify(markEvent));
                markQueue.push('responsePart');
            }
        };

        // Open event for OpenAI WebSocket
        openAiWs.on('open', () => {
            console.log('Connected to the OpenAI Realtime API');
            setTimeout(initializeSession, 100);
        });

        // Listen for messages from the OpenAI WebSocket (and send to Twilio if necessary)
        openAiWs.on('message', (data) => {
            try {
                const response = JSON.parse(data);

                if (LOG_EVENT_TYPES.includes(response.type)) {
                    console.log(`Received event: ${response.type}`, response);
                }

                if (response.type === 'response.audio.delta' && response.delta) {
                    const audioDelta = {
                        event: 'media',
                        streamSid: streamSid,
                        media: { payload: Buffer.from(response.delta, 'base64').toString('base64') }
                    };
                    connection.send(JSON.stringify(audioDelta));

                    // First delta from a new response starts the elapsed time counter
                    if (!responseStartTimestampTwilio) {
                        responseStartTimestampTwilio = latestMediaTimestamp;
                        if (SHOW_TIMING_MATH) console.log(`Setting start timestamp for new response: ${responseStartTimestampTwilio}ms`);
                    }

                    if (response.item_id) {
                        lastAssistantItem = response.item_id;
                    }
                    
                    sendMark(connection, streamSid);
                }

                if (response.type === 'input_audio_buffer.speech_started') {
                    handleSpeechStartedEvent();
                }
            } catch (error) {
                console.error('Error processing OpenAI message:', error, 'Raw message:', data);
            }
        });

        // Handle incoming messages from Twilio
        connection.on('message', (message) => {
            try {
                const data = JSON.parse(message);

                switch (data.event) {
                    case 'media':
                        latestMediaTimestamp = data.media.timestamp;
                        if (SHOW_TIMING_MATH) console.log(`Received media message with timestamp: ${latestMediaTimestamp}ms`);
                        if (openAiWs.readyState === WebSocket.OPEN) {
                            const audioAppend = {
                                type: 'input_audio_buffer.append',
                                audio: data.media.payload
                            };
                            openAiWs.send(JSON.stringify(audioAppend));
                        }
                        break;
                    case 'start':
                        streamSid = data.start.streamSid;
                        console.log('Incoming stream has started', streamSid);

                        // Reset start and media timestamp on a new stream
                        responseStartTimestampTwilio = null; 
                        latestMediaTimestamp = 0;
                        break;
                    case 'mark':
                        if (markQueue.length > 0) {
                            markQueue.shift();
                        }
                        break;
                    default:
                        console.log('Received non-media event:', data.event);
                        break;
                }
            } catch (error) {
                console.error('Error parsing message:', error, 'Message:', message);
            }
        });

        // Handle connection close
        connection.on('close', () => {
            if (openAiWs.readyState === WebSocket.OPEN) openAiWs.close();
            console.log('Client disconnected.');
        });

        // Handle WebSocket close and errors
        openAiWs.on('close', () => {
            console.log('Disconnected from the OpenAI Realtime API');
        });

        openAiWs.on('error', (error) => {
            console.error('Error in the OpenAI WebSocket:', error);
        });
    });
});

fastify.listen({ port: PORT }, (err) => {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    console.log(`Server is listening on port ${PORT}`);
});
