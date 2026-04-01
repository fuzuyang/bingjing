import os

# 定义前端 HTML 模板
# 优化点：引入响应式侧边栏历史记录、流式加载动画、以及更深度的 Markdown 样式美化
html_template = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>营商环境风险评估系统 | 专家版</title>

    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">

    <style>
        @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+SC:wght@400;500;700;900&display=swap');

        :root {
            --primary: #2563eb;
            --bg-soft: #f8fafc;
        }

        body { 
            font-family: 'Noto Sans SC', sans-serif; 
            background-color: var(--bg-soft);
        }

        /* Markdown 深度美化 */
        .report-content { color: #334155; line-height: 1.8; }
        .report-content h1 { font-size: 2.25rem; font-weight: 900; color: #0f172a; margin-bottom: 2rem; border-bottom: 4px solid var(--primary); display: inline-block; }
        .report-content h2 { font-size: 1.4rem; font-weight: 700; color: #1e293b; margin: 2rem 0 1rem; padding: 0.5rem 1rem; background: #eff6ff; border-left: 4px solid var(--primary); border-radius: 0 8px 8px 0; }
        .report-content h3 { font-size: 1.1rem; font-weight: 700; color: #475569; margin-top: 1.5rem; }
        .report-content p { margin-bottom: 1rem; }
        .report-content ul { list-style: none; padding-left: 0; margin-bottom: 1.5rem; }
        .report-content li { position: relative; padding-left: 1.5rem; margin-bottom: 0.5rem; }
        .report-content li::before { content: "•"; color: var(--primary); font-weight: bold; position: absolute; left: 0; }
        .report-content strong { color: #e11d48; font-weight: 600; }
        .report-content hr { margin: 2rem 0; border: 0; border-top: 1px dashed #cbd5e1; }

        /* 动画效果 */
        .fade-in { animation: fadeIn 0.5s ease-out forwards; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }

        /* 隐藏滚动条但保留功能 */
        .no-scrollbar::-webkit-scrollbar { display: none; }
    </style>
</head>
<body class="min-h-screen text-slate-800">

    <div class="flex h-screen overflow-hidden">

        <aside class="w-80 bg-white border-r border-slate-200 flex flex-col hidden md:flex">
            <div class="p-6 border-b border-slate-100">
                <h2 class="text-xl font-black tracking-tighter text-blue-600">评估历史</h2>
                <p class="text-xs text-slate-400 mt-1">最近 10 次评估记录</p>
            </div>
            <div id="historyList" class="flex-1 overflow-y-auto p-4 space-y-3 no-scrollbar">
                <div class="text-center py-10 text-slate-300 text-sm">暂无历史记录</div>
            </div>
            <div class="p-4 bg-slate-50 text-[10px] text-slate-400 text-center">
                System Version v2.0.0 | Web Backend Only
            </div>
        </aside>

        <main class="flex-1 flex flex-col overflow-hidden">

            <header class="bg-white/80 backdrop-blur-md border-b border-slate-200 px-8 py-4 flex justify-between items-center z-10">
                <div class="flex items-center space-x-3">
                    <div class="w-8 h-8 bg-blue-600 rounded-lg flex items-center justify-center text-white font-black">R</div>
                    <span class="font-bold text-slate-700 tracking-tight">营商环境风险 AI 评估流水线</span>
                </div>
                <div class="flex items-center space-x-4">
                    <span id="statusBadge" class="flex items-center space-x-1.5 text-xs text-green-600 font-bold bg-green-50 px-3 py-1.5 rounded-full border border-green-100">
                        <span class="w-2 h-2 bg-green-500 rounded-full animate-pulse"></span>
                        <span>后端 API 在线</span>
                    </span>
                    <button onclick="window.print()" class="text-slate-400 hover:text-blue-600 transition-colors"><i class="fa-solid fa-print"></i></button>
                </div>
            </header>

            <div class="flex-1 overflow-y-auto p-8 no-scrollbar bg-slate-50">
                <div class="max-w-5xl mx-auto grid grid-cols-1 lg:grid-cols-1 gap-8">

                    <div class="bg-white rounded-3xl shadow-sm border border-slate-200 p-8">
                        <div class="flex items-center justify-between mb-4">
                            <h3 class="text-sm font-black text-slate-400 uppercase tracking-widest">争议案件描述</h3>
                            <button onclick="loadExample()" class="text-xs text-blue-500 hover:underline">加载示例案情</button>
                        </div>
                        <textarea id="userInput" rows="6" 
                            class="w-full p-6 bg-slate-50 border border-slate-100 rounded-2xl focus:ring-4 focus:ring-blue-100 focus:border-blue-500 outline-none transition-all text-slate-700 text-base leading-relaxed"
                            placeholder="请描述具体事件，如：某初创企业因无人驾驶路测被监管部门处罚..."></textarea>

                        <div class="mt-6 flex justify-end">
                            <button id="btn" onclick="executeAssessment()"
                                class="bg-blue-600 hover:bg-blue-700 text-white font-bold py-4 px-10 rounded-2xl shadow-xl shadow-blue-200 transition-all flex items-center space-x-3">
                                <i class="fa-solid fa-microchip"></i>
                                <span id="btnText">开始全链路深度评估</span>
                            </button>
                        </div>
                    </div>

                    <div id="loader" class="hidden py-20 text-center fade-in">
                        <div class="inline-block w-16 h-16 border-4 border-blue-600 border-t-transparent rounded-full animate-spin mb-6"></div>
                        <h4 class="text-2xl font-black text-slate-800">正在启动专家评估流水线...</h4>
                        <div class="flex justify-center space-x-8 mt-4 text-xs font-bold text-slate-400">
                            <span id="step1">1.意图识别</span>
                            <span id="step2">2.混合检索</span>
                            <span id="step3">3.风险量化</span>
                        </div>
                    </div>

                    <div id="resultArea" class="hidden space-y-8 fade-in">
                        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <div class="bg-white p-6 rounded-3xl shadow-sm border border-slate-100 flex items-center space-x-4">
                                <div class="w-12 h-12 bg-rose-50 rounded-2xl flex items-center justify-center text-rose-600 text-xl"><i class="fa-solid fa-shield-halved"></i></div>
                                <div>
                                    <p class="text-[10px] font-bold text-slate-400 uppercase">评估结论耗时</p>
                                    <p id="duration" class="text-xl font-black text-slate-800">--</p>
                                </div>
                            </div>
                            <div class="bg-blue-600 p-6 rounded-3xl shadow-sm flex items-center space-x-4 text-white">
                                <div class="w-12 h-12 bg-white/20 rounded-2xl flex items-center justify-center text-white text-xl"><i class="fa-solid fa-bolt"></i></div>
                                <div>
                                    <p class="text-[10px] font-bold text-blue-200 uppercase">数据引擎状态</p>
                                    <p class="text-xl font-black">1942 条政策对撞中</p>
                                </div>
                            </div>
                        </div>

                        <div class="bg-white rounded-3xl shadow-sm border border-slate-200 p-10 md:p-16 relative overflow-hidden">
                            <div class="absolute top-0 left-0 w-full h-2 bg-gradient-to-r from-blue-600 to-indigo-600"></div>
                            <div id="reportBox" class="report-content"></div>
                        </div>
                    </div>

                    <div id="emptyTip" class="py-32 text-center opacity-30">
                        <i class="fa-solid fa-file-invoice text-6xl mb-4"></i>
                        <p class="text-lg font-medium">输入案情并点击上方按钮，即可生成专家级评估报告</p>
                    </div>

                </div>
            </div>
        </main>
    </div>

    <script>
        const API_BASE = "http://127.0.0.1:5050/api/v1";

        // 初始化加载历史记录
        window.onload = fetchHistory;

        function loadExample() {
            document.getElementById('userInput').value = "某初创人工智能企业研发了一款“无人巡检机器人”，在某工业园区试点。当地城管部门以“未经审批”为由，扣押设备并开出50万罚款（企业年营收仅100万）。企业反馈：审批目录无此项，无法办证；执法未经过风险评估，且大型国企同类设备未受罚。";
        }

        async function fetchHistory() {
            try {
                const res = await fetch(`${API_BASE}/risk/history?size=10`);
                const json = await res.json();
                if (json.success) {
                    const list = document.getElementById('historyList');
                    list.innerHTML = json.data.list.map(item => `
                        <div class="p-4 bg-slate-50 rounded-2xl border border-slate-100 hover:border-blue-300 cursor-pointer transition-all">
                            <p class="text-xs font-bold text-slate-800 truncate">${item.summary}</p>
                            <div class="flex justify-between items-center mt-2">
                                <span class="px-2 py-0.5 bg-blue-100 text-blue-600 text-[10px] font-black rounded-md">${item.level}</span>
                                <span class="text-[10px] text-slate-400">${item.time}</span>
                            </div>
                        </div>
                    `).join('');
                }
            } catch (e) { console.error("History fetch failed"); }
        }

        async function executeAssessment() {
            const userInput = document.getElementById('userInput').value;
            const btn = document.getElementById('btn');

            if (!userInput.trim()) return alert("请输入案情描述！");

            // UI 状态切换
            btn.disabled = true;
            document.getElementById('loader').classList.remove('hidden');
            document.getElementById('emptyTip').classList.add('hidden');
            document.getElementById('resultArea').classList.add('hidden');

            try {
                // 模拟步骤动画效果
                setTimeout(() => document.getElementById('step1').classList.add('text-blue-600'), 500);
                setTimeout(() => document.getElementById('step2').classList.add('text-blue-600'), 1500);

                const response = await fetch(`${API_BASE}/risk/evaluate`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ message: userInput })
                });

                const res = await response.json();

                if (res.success) {
                    // 渲染耗时
                    document.getElementById('duration').innerText = res.data.metrics.duration;
                    // 渲染 Markdown
                    document.getElementById('reportBox').innerHTML = marked.parse(res.data.markdown);

                    document.getElementById('resultArea').classList.remove('hidden');
                    fetchHistory(); // 刷新左侧历史
                } else {
                    alert("评估失败: " + res.message);
                }
            } catch (err) {
                alert("无法连接到后端，请确保 app.py 在 5050 端口运行。");
            } finally {
                btn.disabled = false;
                document.getElementById('loader').classList.add('hidden');
            }
        }
    </script>
</body>
</html>
"""

# 执行构建：将代码写入 index.html
try:
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html_template)
    print(" 网页前端 `index.html` 已重构完成！")
    print(" 亮点：")
    print("   1. 完美对接 /api/v1 路由规范")
    print("   2. 增加左侧评估历史预览功能")
    print("   3. 优化 Markdown 样式，具备真实的专家报告质感")
except Exception as e:
    print(f" 生成失败: {e}")