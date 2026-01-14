import os
import requests
import time
import json
from flask import Flask, request, jsonify, render_template_string

# --- 配置 ---
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# --- 完整的聊天机器人UI模板 ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Medicare Monthly Enrollment</title>
    <script src="https://unpkg.com/lucide@latest"></script>
    <style>
        :root {
            --bg-color: #ffffff;
            --text-color: #1a1a1a;
            --border-color: #e0e0e0;
            --placeholder-color: #6b7280;
            --bot-bg: #f7f7f7;
            --accent-color: #6366f1;
        }
        body {
            margin: 0;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background-color: var(--bg-color);
            color: var(--text-color);
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
        }
        .chat-container {
            width: 100%;
            max-width: 800px;
            height: 90vh;
            max-height: 800px;
            display: flex;
            flex-direction: column;
            border: 1px solid var(--border-color);
            border-radius: 12px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.05);
            overflow: hidden;
        }
        .chat-header {
            padding: 1rem;
            border-bottom: 1px solid var(--border-color);
            font-weight: 600;
            font-size: 1.1rem;
        }
        .chat-messages {
            flex-grow: 1;
            overflow-y: auto;
            padding: 1.5rem;
            display: flex;
            flex-direction: column;
        }
        .welcome-screen {
            text-align: center;
            margin: auto;
        }
        .welcome-icon {
            width: 60px;
            height: 60px;
            background: linear-gradient(135deg, #a78bfa, #6366f1);
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            margin: 0 auto 1rem;
            color: white;
        }
        .welcome-screen h2 { font-size: 1.5rem; margin-bottom: 0.5rem; }
        .sample-questions {
            margin-top: 2rem;
            display: flex;
            flex-direction: column;
            gap: 0.75rem;
            align-items: flex-start;
            margin-left: auto;
            margin-right: auto;
            max-width: 90%;
        }
        .sample-question {
            padding: 0.6rem 1rem;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            cursor: pointer;
            transition: background-color 0.2s;
            font-size: 0.9rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        .sample-question:hover { background-color: #f9fafb; }
        .message {
            max-width: 85%;
            padding: 0.75rem 1.25rem;
            border-radius: 18px;
            margin-bottom: 1rem;
            line-height: 1.5;
            white-space: pre-wrap;
            font-family: 'Menlo', 'Consolas', monospace;
            font-size: 0.9rem;
        }
        .user-message {
            background-color: var(--accent-color);
            color: white;
            align-self: flex-end;
            border-bottom-right-radius: 4px;
        }
        .bot-message {
            background-color: var(--bot-bg);
            color: var(--text-color);
            align-self: flex-start;
            border-bottom-left-radius: 4px;
        }
        .typing-indicator {
            align-self: flex-start;
            display: flex;
            gap: 4px;
            padding: 0.75rem 1.25rem;
        }
        .typing-indicator span {
            width: 8px;
            height: 8px;
            background-color: #ccc;
            border-radius: 50%;
            animation: bounce 1s infinite;
        }
        .typing-indicator span:nth-child(2) { animation-delay: 0.2s; }
        .typing-indicator span:nth-child(3) { animation-delay: 0.4s; }
        @keyframes bounce { 0%, 80%, 100% { transform: scale(0); } 40% { transform: scale(1); } }
        .chat-input-area {
            border-top: 1px solid var(--border-color);
            padding: 1rem;
            display: flex;
            gap: 0.5rem;
        }
        .chat-input-area textarea {
            flex-grow: 1;
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 0.75rem;
            font-size: 1rem;
            resize: none;
            font-family: inherit;
        }
        .chat-input-area textarea:focus { outline: none; border-color: var(--accent-color); }
        .chat-input-area button {
            border: none;
            background-color: var(--accent-color);
            color: white;
            padding: 0 1rem;
            border-radius: 8px;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
        }
    </style>
</head>
<body>
    <div class="chat-container">
        <div class="chat-messages" id="chat-messages">
            <div class="welcome-screen" id="welcome-screen">
                <div class="welcome-icon"><i data-lucide="bot"></i></div>
                <h2>Medicare Monthly Enrollment</h2>
                <div class="sample-questions">
                    <div class="sample-question" onclick="askSample(this)">
                        <i data-lucide="table-2" width="16"></i>
                        <span>What tables are there and how are they connected? Give me a short summary.</span>
                    </div>
                    <div class="sample-question" onclick="askSample(this)">
                        <i data-lucide="hash" width="16"></i>
                        <span>What are the minimum, maximum, and average monthly enrollments for Prescription Drug Plans across all counties?</span>
                    </div>
                    <div class="sample-question" onclick="askSample(this)">
                        <i data-lucide="pie-chart" width="16"></i>
                        <span>What is the distribution of Medicare beneficiaries by coverage type (Original Medicare vs Medicare Advantage) at the state level?</span>
                    </div>
                    <div class="sample-question" onclick="askSample(this)">
                        <i data-lucide="trending-up" width="16"></i>
                        <span>What is the monthly trend in total number of Medicare beneficiaries enrolled nationally?</span>
                    </div>
                </div>
            </div>
        </div>
        <div class="chat-input-area">
            <textarea id="userInput" placeholder="Ask your question..." rows="1" onkeydown="handleEnter(event)"></textarea>
            <button id="sendButton" onclick="sendMessage()"><i data-lucide="send"></i></button>
        </div>
    </div>

    <script>
        lucide.createIcons();
        const chatMessages = document.getElementById('chat-messages');
        const userInput = document.getElementById('userInput');
        const welcomeScreen = document.getElementById('welcome-screen');

        function askSample(element) {
            const question = element.querySelector('span').innerText;
            userInput.value = question;
            sendMessage();
        }

        function handleEnter(event) {
            if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault();
                sendMessage();
            }
        }

        function addMessage(content, sender) {
            const messageElement = document.createElement('div');
            messageElement.classList.add('message', `${sender}-message`);
            messageElement.innerText = content;
            chatMessages.appendChild(messageElement);
            chatMessages.scrollTop = chatMessages.scrollHeight;
        }

        function showTypingIndicator() {
            const indicator = document.createElement('div');
            indicator.id = 'typing-indicator';
            indicator.classList.add('typing-indicator');
            indicator.innerHTML = '<span></span><span></span><span></span>';
            chatMessages.appendChild(indicator);
            chatMessages.scrollTop = chatMessages.scrollHeight;
        }

        function removeTypingIndicator() {
            const indicator = document.getElementById('typing-indicator');
            if (indicator) {
                indicator.remove();
            }
        }

        async function sendMessage() {
            const question = userInput.value.trim();
            if (!question) return;

            if (welcomeScreen) {
                welcomeScreen.style.display = 'none';
            }

            addMessage(question, 'user');
            userInput.value = '';
            showTypingIndicator();

            try {
                const res = await fetch('/ask', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ question: question })
                });
                const data = await res.json();
                
                removeTypingIndicator();

                if (data.error) {
                    addMessage(`Error: ${data.details || data.error}`, 'bot');
                } else {
                    addMessage(data.answer, 'bot');
                }
            } catch (error) {
                removeTypingIndicator();
                addMessage('An unexpected error occurred: ' + error, 'bot');
            }
        }
    </script>
</body>
</html>
"""

app = Flask(__name__)

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/ask', methods=['POST'])
def ask():
    if not all([DATABRICKS_HOST, GENIE_SPACE_ID, DATABRICKS_TOKEN]):
        return jsonify({"error": "Server is not configured. Missing environment variables."}), 500
    user_question = request.json.get('question')
    if not user_question:
        return jsonify({'error': 'Question cannot be empty'}), 400
    try:
        headers = {'Authorization': f'Bearer {DATABRICKS_TOKEN}', 'Content-Type': 'application/json'}
        
        start_conv_url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/start-conversation"
        start_payload = {'content': user_question}
        start_response = requests.post(start_conv_url, headers=headers, json=start_payload)
        start_response.raise_for_status()
        start_data = start_response.json()
        conversation_id = start_data['conversation']['id']
        message_id = start_data['message']['id']
        
        message_url = f"{DATABRICKS_HOST}/api/2.0/genie/spaces/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}"
        status = ""
        poll_data = {}
        start_time = time.time()
        while status not in ['COMPLETED', 'FAILED', 'CANCELLED'] and time.time() - start_time < 300:
            time.sleep(3)
            poll_response = requests.get(message_url, headers=headers)
            poll_response.raise_for_status()
            poll_data = poll_response.json()
            status = poll_data.get('status')
        
        if status == 'COMPLETED':
            # --- 从这里开始是修改后的逻辑 ---
            answer_parts = []
            
            # 遍历所有附件，收集文本和数据
            for attachment in poll_data.get('attachments', []):
                # 1. 如果是文本附件，直接添加
                if 'text' in attachment:
                    answer_parts.append(attachment['text'])
                
                # 2. 如果是数据查询附件，获取数据并格式化为表格
                if 'query' in attachment and 'statement_id' in attachment['query']:
                    statement_id = attachment['query']['statement_id']
                    results_url = f"{DATABRICKS_HOST}/api/2.0/sql/statements/{statement_id}"
                    results_response = requests.get(results_url, headers=headers)
                    
                    if results_response.status_code == 200:
                        results_data = results_response.json()
                        # 确保有数据可供格式化
                        if 'result' in results_data and 'data_array' in results_data['result'] and len(results_data['result']['data_array']) > 0:
                            columns = [col['name'] for col in results_data['manifest']['schema']['columns']]
                            data_array = results_data['result']['data_array']
                            
                            header = " | ".join(columns)
                            separator = " | ".join(["---"] * len(columns))
                            rows = [" | ".join(map(str, row)) for row in data_array]
                            
                            table_text = "\n".join([header, separator] + rows)
                            answer_parts.append(table_text)

            # 将收集到的所有部分用换行符连接起来
            # 如果什么都没有，就返回一个默认消息
            if answer_parts:
                final_answer = "\n\n".join(answer_parts)
                return jsonify({'answer': final_answer})
            else:
                return jsonify({'answer': "I've processed your request, but couldn't find a specific answer or data."})
            # --- 修改逻辑结束 ---

        else:
            return jsonify({'error': f'Failed to get answer. Final status: {status}', 'details': poll_data.get('error')}), 500
    except requests.exceptions.HTTPError as e:
        return jsonify({'error': f"HTTP Error: {e.response.status_code}", 'details': e.response.text}), 500
    except Exception as e:
        return jsonify({'error': f'An unexpected server error occurred: {str(e)}'}), 500



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
