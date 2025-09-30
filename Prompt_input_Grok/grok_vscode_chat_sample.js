using vscode chat api in javascript create a sample code 

to convert user prompt to sql query using llm model
show the sql query in the chat window along with a button continue-  to continue the execution of sql command and cancel button - to cancel and start the chat from start again
If user clicked continue button create a dynamic url string and pass sql query created and receive the response and show it in chat 



// src/extension.js
const vscode = require('vscode');
const fetch = require('node-fetch'); // Add "node-fetch": "^2.6.1" to dependencies in package.json

exports.activate = function(context) {
  const handler = async (request, context, stream, token) => {
    if (request.command === 'continue') {
      const sql = request.prompt.trim();
      stream.progress('Executing SQL...');
      
      // Create dynamic URL
      const url = `https://example.com/api/execute?sql=${encodeURIComponent(sql)}`;
      
      try {
        const response = await fetch(url);
        const result = await response.text();
        stream.markdown(`**Execution Result:**\n\n${result}`);
      } catch (error) {
        stream.markdown(`**Error:** ${error.message}`);
      }
      
      return {}; // No specific result needed here
    } else if (request.command === 'cancel') {
      stream.markdown('Operation cancelled. Start a new query.');
      return {};
    } else {
      // Default: Convert prompt to SQL using LLM
      const models = await vscode.lm.selectChatModels({ vendor: 'copilot', family: 'gpt-4o' });
      if (models.length === 0) {
        stream.markdown('No language model available.');
        return {};
      }
      const model = models[0];
      
      const messages = [
        vscode.LanguageModelChatMessage.User('You are an SQL expert. Convert the following natural language query to a valid SQL query. Respond only with the SQL query, no explanations.'),
        vscode.LanguageModelChatMessage.User(request.prompt)
      ];
      
      stream.progress('Generating SQL...');
      
      const chatResponse = await model.sendRequest(messages, {}, token);
      let sql = '';
      for await (const fragment of chatResponse.text) {
        sql += fragment;
        stream.markdown(fragment); // Stream the SQL as it's generated
      }
      
      sql = sql.trim();
      
      // Show the SQL (already streamed), now add buttons via followups (but since followups are provided separately)
      
      return { sql }; // Return the SQL for followup provider
    }
  };

  const followupProvider = async (turn, result, token) => {
    if (result && result.sql) {
      return [
        {
          prompt: `/continue ${result.sql}`,
          label: 'Continue',
          tooltip: 'Execute the SQL query'
        },
        {
          prompt: '/cancel',
          label: 'Cancel',
          tooltip: 'Cancel and start over'
        }
      ];
    }
    return [];
  };

  const participant = vscode.chat.createChatParticipant('sql-chat.sql-converter', handler);
  participant.followupProvider = followupProvider;

  context.subscriptions.push(participant);
};
