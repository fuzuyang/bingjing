import os
import sys
import time
import io
import json
import logging
import traceback
from datetime import datetime
from functools import wraps
from logging.handlers import RotatingFileHandler

from sqlalchemy import inspect
from flask import Flask, request, jsonify, g, send_from_directory, Response, stream_with_context
from flask_cors import CORS
from werkzeug.datastructures import FileStorage
from werkzeug.middleware.proxy_fix import ProxyFix
from dotenv import load_dotenv
from openai import OpenAI
import pdfplumber

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

load_dotenv(override=True)

# 导入业务核心模块
from core import SessionLocal, RiskAssessment, engine
from modules.evaluator.coordinator import AssessmentCoordinator
from modules.evaluator.self_check import GlobalSelfChecker
from feature2_database_init import LegalOpinion, Complaint


# =========================================================
# 2. 统一响应格式与全局中间件
# =========================================================
class ResponseFormatter:
    """标准化 API 响应格式，确保前端调用一致性"""

    @staticmethod
    def success(data=None, message="Success", code=200):
        return jsonify({
            "code": code,
            "success": True,
            "message": message,
            "data": data,
            "trace_id": g.get('request_id'),
            "timestamp": datetime.now().isoformat()
        }), code

    @staticmethod
    def error(message="Error", code=400, details=None):
        return jsonify({
            "code": code,
            "success": False,
            "message": message,
            "details": details or {},
            "trace_id": g.get('request_id'),
            "timestamp": datetime.now().isoformat()
        }), code


def validate_json(f):
    """强制 JSON 请求校验装饰器"""

    @wraps(f)
    def decorated(*args, **kwargs):
        if not request.is_json:
            return ResponseFormatter.error("Content-Type must be application/json", 400)
        return f(*args, **kwargs)

    return decorated


# =========================================================
# 3. Flask 应用配置 (专注于 Web 性能)
# =========================================================
app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)

# 配置跨域：允许网页端（如 Vue/React）访问
CORS(app, resources={r"/api/*": {"origins": "*"}})


def setup_app_logging():
    """配置滚动日志监控"""
    log_dir = os.path.join(BASE_DIR, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'web_service.log')

    handler = RotatingFileHandler(log_file, maxBytes=20 * 1024 * 1024, backupCount=10, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] - %(message)s')
    handler.setFormatter(formatter)

    app.logger.addHandler(handler)
    app.logger.setLevel(logging.INFO)


setup_app_logging()

MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", 15))
MAX_EXTRACT_CHARS = int(os.getenv("MAX_EXTRACT_CHARS", 20000))
ALLOWED_UPLOAD_EXTS = {".txt", ".md", ".pdf", ".docx", ".csv"}
ELEMENT_PARSER_MODEL = os.getenv("ELEMENT_PARSER_MODEL", "deepseek-chat")
ELEMENT_PROMPT_LAWYER_OPINION = """你是具备证券与诉讼法律文书专业解析能力的智能助手，仅处理诉讼/仲裁相关法律意见书PDF文档，严格遵循以下规则执行解析任务，最终仅输出标准JSON格式内容，无任何前置说明、后置解释、备注话术，输出结果可直接填入Excel法律意见书对应字段。

固定提取字段与规则（严格按以下顺序执行，不得增减、调整字段）
| 序号 | 字段名 | 精准提取规则 |
|------|--------|--------------|
| 1 | 文书标题 | 提取文档开篇载明的法律意见书完整全称，不得简写、缩写 |
| 2 | 文书类型 | 从《》中精准匹配选择，《一、法定强制类法律意见书
（一）资本市场与证券业务类
1. IPO/北交所挂牌法律意见书
2. 上市公司重大资产重组法律意见书
3. 上市公司重大资产重组专项核查法律意见书
4. 上市公司定增、可转债、配股、公开发行再融资法律意见书
5. 上市公司收购、要约收购法律意见书
6. 上市公司权益变动法律意见书
7. 上市公司股份回购法律意见书
8. 上市公司关联交易专项核查法律意见书
9. 上市公司资金占用专项核查法律意见书
10. 上市公司退市法律意见书
11. 上市公司破产重整法律意见书
12. 新三板挂牌法律意见书
13. 新三板定向发行法律意见书
14. 新三板重大资产重组法律意见书
15. 公司债发行法律意见书
16. 企业债发行法律意见书
17. 中期票据发行法律意见书
18. 短期融资券发行法律意见书
19. 资产证券化（ABS）发行法律意见书
20. 公募REITs发行法律意见书

（二）金融机构与资管监管类
1. 私募基金管理人登记专项法律意见书
2. 私募基金管理人重大事项变更专项法律意见书
3. 银行机构设立、增资、股权变更法律意见书
4. 保险机构设立、增资、股权变更法律意见书
5. 信托机构设立、增资、股权变更法律意见书
6. 券商机构设立、增资、股权变更法律意见书
7. 基金公司设立、增资、股权变更法律意见书
8. 金融机构业务资质申请法律意见书
9. 金融机构风险处置专项法律意见书
10. 资管产品备案法律意见书

（三）国资与国企监管类
1. 国有企业改制法律意见书
2. 国有企业混合所有制改革法律意见书
3. 国有产权转让法律意见书
4. 国有企业增资扩股法律意见书
5. 国有企业重大资产交易法律意见书
6. 国有企业重大投资专项法律意见书
7. 国有企业合规体系建设专项法律意见书

（四）其他法定强制类
1. 经营者集中反垄断申报法律意见书
2. 境外直接投资（ODI）法律意见书
3. 外商投资（FDI）备案/核准法律意见书
4. 企业破产清算法律意见书
5. 企业破产和解法律意见书
6. 招投标合规法律意见书
7. 政府采购合规法律意见书
8. 企业刑事合规不起诉专项法律意见书

二、非法定专项服务类法律意见书
（一）争议解决专项类
1. 诉讼案件风险分析法律意见书
2. 仲裁案件风险分析法律意见书
3. 执行程序专项法律意见书
4. 财产保全专项法律意见书
5. 和解调解方案专项法律意见书
6. 涉诉事项影响专项核查法律意见书

（二）商业交易类
1. 股权投资尽职调查法律意见书
2. 并购交易尽职调查法律意见书
3. 资产转让法律风险法律意见书
4. 特许经营专项法律意见书
5. 项目合作专项法律意见书
6. 重大合同履约风险法律意见书
7. 企业投融资专项法律意见书
8. 担保事项合法性法律意见书

（三）合规与风险防控类
1. 数据合规专项法律意见书
2. 个人信息保护专项法律意见书
3. 网络安全专项法律意见书
4. 反垄断合规法律意见书
5. 反不正当竞争合规法律意见书
6. 广告宣传合规法律意见书
7. 劳动用工合规法律意见书
8. 税务合规法律意见书
9. 环保合规法律意见书
10. 安全生产合规法律意见书
11. 出口管制与经济制裁合规法律意见书

（四）知识产权专项类
1. 知识产权权属法律意见书
2. 专利侵权风险法律意见书
3. 商标侵权风险法律意见书
4. 著作权侵权风险法律意见书
5. 专利稳定性法律意见书
6. 商标稳定性法律意见书
7. 商业秘密保护专项法律意见书

（五）其他专项类
1. 家族财富传承专项法律意见书
2. 家族信托设立专项法律意见书
3. 企业解散、清算、注销专项法律意见书
4. 特定行为合法性分析法律意见书
5. 企业年度合规法律意见书》 |
| 3 | 出具机构 | 提取出具本法律意见书的律师事务所完整全称 |
| 4 | 委托人 | 提取法律意见书致送的委托方/委托单位完整全称 |
| 5 | 出具日期 | 提取文档签署页载明的完整出具年月日，无明确标注则填「未明确标注」 |
| 6 | 案件名称 | 提取文档涉及的全部诉讼/仲裁案件完整名称，多个案件需全部列明，不得遗漏 |
| 7 | 案由 | 提取案件对应的法定民事/商事案由，如侵害商业秘密纠纷、合同纠纷等，多个案件对应案由全部列明 |
| 8 | 法院/仲裁机构 | 提取案件对应的受理法院/仲裁机构完整全称，多个案件对应机构全部列明 |
| 9 | 案号 | 提取案件对应的完整正式案号，多个案件案号全部列明 |
| 10 | 程序阶段 | 提取文档载明的案件当前所处诉讼/仲裁程序阶段，如一审已受理未开庭、未进入诉讼程序等 |
| 11 | 当事人 | 完整列明案件全部原告、被告、第三人等诉讼参与主体全称，并标注对应诉讼身份 |
| 12 | 关键事实 | 基于文档原文，完整还原案件核心时间线、主体行为、权利义务约定、涉诉核心事件等关键事实，不得添加主观推断 |
| 13 | 争议焦点 | 提取案件双方核心的权利义务分歧、主张冲突、核心争议事项 |
| 14 | 适用法律 | 提取法律意见书中明确引用的全部法律法规、部门规章、规范性文件完整名称 |
| 15 | 律师分析 | 完整提取经办律师针对案件事实、法律适用、双方主张、证据效力作出的专业分析内容 |
| 16 | 律师结论 | 完整提取律师针对案件出具的最终明确结论性意见 |
| 17 | 风险等级 | 基于律师结论，划分为「低风险/中风险/高风险」；律师明确认定主张依据不足、无败诉风险的填「低风险」，存在败诉可能的填「中风险」，大概率败诉且影响重大的填「高风险」 |
| 18 | 是否重大不利影响 | 提取法律意见书中明确载明的结论，原文标注「不会造成重大不利影响」则填「否」，反之填「是」，无明确表述则填「未明确判定」 |
| 19 | 送达/核查材料 | 完整提取文档中列明的法院送达诉讼材料、律师核查验证的全部材料清单 |
| 20 | 附件清单 | 提取文档末尾列明的全部附件，无附件则填「无」 |

强制输出要求
1. 文档涉及多个案件的，每个字段需完整涵盖所有案件的对应信息，不得遗漏；
2. 无对应内容的字段，统一填「无」，不得留空；
3. 所有提取内容必须完全来自文档原文，不得添加任何文档外信息、主观推断内容；
4. 最终输出仅为标准JSON格式，键名与上述字段名完全一致，值为对应提取内容，无任何额外文本。
"""
ELEMENT_PROMPT_COMPLAINT = """
你是具备民商事诉讼文书专业解析能力的智能助手，仅处理民事起诉状PDF文档/含起诉状核心内容的涉诉公告PDF文档，严格遵循以下规则执行解析任务，最终仅输出标准JSON格式内容，无任何前置说明、后置解释、备注话术，输出结果可直接填入Excel起诉状对应字段。

固定提取字段与规则（严格按以下顺序执行，不得增减、调整字段）
| 序号 | 字段名 | 精准提取规则 |
|------|--------|--------------|
| 1 | 文书名称 | 提取文档载明的起诉状完整名称，涉诉公告则填公告中载明的涉诉文书全称 |
| 2 | 法院 | 提取受理本案的人民法院完整全称 |
| 3 | 案号 | 提取本案对应的完整正式案号 |
| 4 | 原告 | 完整列明起诉状中全部原告的主体全称、住所地、法定代表人信息 |
| 5 | 被告 | 完整列明起诉状中全部被告的主体全称、住所地、法定代表人信息 |
| 6 | 第三人 | 完整列明起诉状中全部第三人的主体信息，无第三人则填「无」 |
| 7 | 案由 | 提取本案对应的法定民商事案由，如借款合同纠纷等 |
| 8 | 诉讼请求全文 | 提取起诉状中载明的全部诉讼请求完整原文，不得做任何删减、修改 |
| 9 | 请求分项 | 将全部诉讼请求按原文序号拆解为独立条目，逐条清晰列明 |
| 10 | 本金 | 提取诉讼请求中主张的本金金额，精确到分，无本金主张则填「无」 |
| 11 | 利息 | 完整提取诉讼请求中主张的利息、逾期利息金额、计算基数、年利率标准、计算期间，无利息主张则填「无」 |
| 12 | 违约金 | 完整提取诉讼请求中主张的违约金金额、计算标准，无违约金主张则填「无」 |
| 13 | 律师费 | 提取诉讼请求中主张的律师费金额，无律师费主张则填「无」 |
| 14 | 保全费 | 提取诉讼请求中主张的保全费、保全担保费等相关费用，无相关主张则填「无」 |
| 15 | 优先受偿 | 完整提取诉讼请求中关于优先受偿权的全部主张，包括对应财产/权利范围、优先受偿的债权范围，无相关主张则填「无」 |
| 16 | 连带责任 | 完整提取诉讼请求中关于连带清偿责任的全部主张，包括责任主体、承担责任的范围，无相关主张则填「无」 |
| 17 | 事实与理由全文 | 提取起诉状中「事实与理由」部分的完整原文，不得做任何删减、修改 |
| 18 | 关键合同 | 完整列明本案涉及的全部核心合同/协议，包括合同全称、签署主体、签署时间 |
| 19 | 关键时间 | 按时间先后顺序，完整列明本案核心事件的全部关键时间节点 |
| 20 | 当前进展 | 提取文档载明的本案当前所处的诉讼程序阶段，如已受理未开庭等 |

强制输出要求
1. 无对应内容的字段，统一填「无」，不得留空；
2. 所有提取内容必须完全来自文档原文，不得添加任何文档外信息、主观推断内容；
3. 若文档为涉诉公告，需从公告中精准提取起诉状对应的全部信息，不得遗漏公告载明的起诉状核心内容；
4. 最终输出仅为标准JSON格式，键名与上述字段名完全一致，值为对应提取内容，无任何额外文本。
"""


def _parse_document_elements_with_llm(document_text: str, prompt_template: str):
    api_key = os.getenv("SILICONFLOW_API_KEY")
    if not api_key:
        raise ValueError("SILICONFLOW_API_KEY is not configured")

    client = OpenAI(
        api_key=api_key,
        base_url="https://api.deepseek.com/v1",
    )

    safe_text = _truncate_text(document_text or "")
    response = client.chat.completions.create(
        model=ELEMENT_PARSER_MODEL,
        messages=[
            {"role": "system", "content": prompt_template},
            {"role": "user", "content": f"请基于以下文档内容提取要素，仅返回JSON：\n{safe_text}"},
        ],
        response_format={"type": "json_object"},
        temperature=0.2,
        max_tokens=1800,
    )

    content = ((response.choices[0].message.content if response and response.choices else "") or "").strip()
    if not content:
        return {}

    try:
        return json.loads(content)
    except json.JSONDecodeError:
        # 保底返回原始文本，避免阻塞后续流程
        return {"raw_output": content}


def _truncate_text(text: str, max_chars: int = MAX_EXTRACT_CHARS) -> str:
    if not text:
        return ""
    text = text.strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n\n[内容已截断]"


def _decode_text_bytes(raw: bytes) -> str:
    if not raw:
        return ""
    for enc in ("utf-8", "utf-8-sig", "gbk"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")


def _extract_text_from_uploaded_file(uploaded_file: FileStorage):
    if not uploaded_file:
        return "", None

    filename = uploaded_file.filename or ""
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_UPLOAD_EXTS:
        return None, f"不支持的文件类型: {ext or 'unknown'}，仅支持 {', '.join(sorted(ALLOWED_UPLOAD_EXTS))}"

    raw = uploaded_file.read() or b""
    size_mb = len(raw) / (1024 * 1024)
    if size_mb > MAX_UPLOAD_MB:
        return None, f"上传文件过大({size_mb:.2f}MB)，限制 {MAX_UPLOAD_MB}MB"
    if not raw:
        return None, "上传文件为空"

    try:
        if ext in {".txt", ".md", ".csv"}:
            return _truncate_text(_decode_text_bytes(raw)), None

        if ext == ".pdf":
            from pypdf import PdfReader

            reader = PdfReader(io.BytesIO(raw))
            text = "\n".join((page.extract_text() or "") for page in reader.pages)
            return _truncate_text(text), None

        if ext == ".docx":
            try:
                from docx import Document
            except Exception:
                return None, "缺少 python-docx 依赖，暂不支持 docx 解析"

            doc = Document(io.BytesIO(raw))
            text = "\n".join(p.text.strip() for p in doc.paragraphs if p.text and p.text.strip())
            return _truncate_text(text), None
    except Exception as exc:
        return None, f"文件解析失败: {str(exc)}"

    return None, "未能解析该文件"


def _build_case_description_from_request():
    """
    同时支持:
    1) application/json: {"message": "..."}
    2) multipart/form-data: message + file
    """
    extracted_file_name = None
    extracted_chars = 0

    if request.is_json:
        data = request.get_json(silent=True) or {}
        message = (data.get("message") or "").strip()
        return message, {"input_mode": "text", "file_name": None, "extracted_chars": 0}, None

    content_type = (request.content_type or "").lower()
    if "multipart/form-data" not in content_type:
        return None, None, "仅支持 application/json 或 multipart/form-data"

    message = (request.form.get("message") or "").strip()
    uploaded_file = request.files.get("file")
    file_text = ""

    if uploaded_file and uploaded_file.filename:
        extracted_file_name = uploaded_file.filename
        file_text, error = _extract_text_from_uploaded_file(uploaded_file)
        if error:
            return None, None, error
        extracted_chars = len(file_text or "")

    parts = []
    if message:
        parts.append(message)
    if file_text:
        parts.append(f"【上传文件解析文本 - {extracted_file_name}】\n{file_text}")

    merged = "\n\n".join(parts).strip()
    mode = "text+file" if (message and file_text) else ("file" if file_text else "text")
    return merged, {
        "input_mode": mode,
        "file_name": extracted_file_name,
        "extracted_chars": extracted_chars,
    }, None


@app.route('/', methods=['GET'])
def frontend_index():
    """提供前端主页"""
    return send_from_directory(BASE_DIR, 'index.html')


@app.route('/upload/document/test', methods=['GET'])
def upload_document_test_page():
    """提供 /upload/document 路由测试页"""
    return send_from_directory(BASE_DIR, 'upload_document_test.html')


@app.before_request
def start_timer():
    """性能埋点：记录请求开始时间与唯一标识"""
    g.start_time = time.time()
    g.request_id = os.urandom(8).hex()


@app.teardown_appcontext
def close_db_session(exception=None):
    """事务守卫：确保每个请求结束时释放数据库连接池"""
    SessionLocal.remove()


# =========================================================
# 4. 核心业务接口 (API 端点)
# =========================================================

@app.route('/api/v1/health', methods=['GET'])
def health():
    """系统健康状况监控"""
    return ResponseFormatter.success({
        "status": "up",
        "modules": ["intent_recognizer", "retriever", "analyzer", "risk_evaluator"]
    })


@app.route('/api/v1/self-check', methods=['GET', 'POST'])
def run_self_check():
    """全局自检入口：检查并可选自动修复结构化合规能力"""
    auto_fix = True
    if request.method == "POST" and request.is_json:
        payload = request.get_json(silent=True) or {}
        auto_fix = bool(payload.get("auto_fix", True))
    elif request.method == "GET":
        raw = (request.args.get("auto_fix", "1") or "1").strip().lower()
        auto_fix = raw not in {"0", "false", "no"}

    try:
        checker = GlobalSelfChecker(auto_fix=auto_fix)
        result = checker.run()
        message = "全局自检通过" if result.get("overall_status") == "passed" else "全局自检未通过"
        return ResponseFormatter.success(result, message)
    except Exception as e:
        app.logger.error(f"[{g.request_id}] 全局自检失败: {str(e)}\n{traceback.format_exc()}")
        return ResponseFormatter.error("全局自检执行失败", 500, {"detail": str(e)})


@app.route('/api/v1/risk/evaluate', methods=['POST'])
def run_evaluation():
    """
    全链路风险评估入口
    逻辑流：用户案情 -> 意图识别 -> 混合检索 -> 精神归纳 -> 风险量化 -> 报告生成 [cite: 2-13]
    """
    case_description, input_meta, parse_error = _build_case_description_from_request()
    if parse_error:
        return ResponseFormatter.error(parse_error, 400)

    # 基础参数校验
    if not case_description or not case_description.strip():
        return ResponseFormatter.error("请输入案情描述", 400)

    def _sse(event: str, payload: dict) -> str:
        return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

    @stream_with_context
    def generate():
        try:
            yield _sse("status", {
                "stage": "start",
                "message": "开始评估",
                "trace_id": g.get("request_id"),
            })

            app.logger.info(f"[{g.request_id}] 启动深度评估：{case_description[:30]}...")
            coordinator = AssessmentCoordinator()

            yield _sse("status", {
                "stage": "intent",
                "message": "正在识别意图",
                "trace_id": g.get("request_id"),
            })
            intent_data = coordinator.recognizer.recognize(case_description)
            if not intent_data:
                raise ValueError("流程中断：未识别到有效意图。")

            yield _sse("status", {
                "stage": "retrieve",
                "message": "正在检索依据",
                "trace_id": g.get("request_id"),
            })
            retrieved_bundle = coordinator.retriever.retrieve(intent_data)

            yield _sse("status", {
                "stage": "analyze",
                "message": "正在进行合规分析",
                "trace_id": g.get("request_id"),
            })
            compliance_analysis = coordinator.analyzer.analyze_compliance(case_description, intent_data, retrieved_bundle)

            yield _sse("status", {
                "stage": "generate",
                "message": "正在生成回答",
                "trace_id": g.get("request_id"),
            })
            generation = {}
            streamed_chars = 0
            for event in coordinator.answer_generator.generate_stream(case_description, intent_data, compliance_analysis):
                event_type = str(event.get("type") or "")
                if event_type == "token":
                    delta = str(event.get("delta") or "")
                    if not delta:
                        continue
                    for ch in delta:
                        streamed_chars += 1
                        yield _sse("token", {
                            "delta": ch,
                            "chars": streamed_chars,
                            "trace_id": g.get("request_id"),
                        })
                elif event_type == "final":
                    generation = event.get("payload") or {}
            if not generation:
                generation = {"mode": "empty", "answer": "", "citations": []}
            compliance_analysis["llm_answer"] = generation

            yield _sse("status", {
                "stage": "evaluate",
                "message": "正在计算风险评级",
                "trace_id": g.get("request_id"),
            })
            evaluation_result = coordinator.evaluator.evaluate(
                case_description,
                intent_data,
                compliance_analysis,
                retrieved_bundle,
            )
            report_md = coordinator.reporter.generate(intent_data, compliance_analysis, evaluation_result)

            coordinator.latest_result = {
                "intent": intent_data,
                "retrieved": retrieved_bundle,
                "analysis": compliance_analysis,
                "generation": generation,
                "evaluation": evaluation_result,
                "markdown": report_md,
                "input_meta": input_meta or {"input_mode": "text", "file_name": None, "extracted_chars": 0},
            }
            coordinator._save_to_db(case_description, coordinator.latest_result)

            cost = time.time() - g.start_time
            app.logger.info(f"[{g.request_id}] 评估完成，耗时: {cost:.2f}s")

            result_payload = {
                "code": 200,
                "success": True,
                "message": "评估回答生成成功",
                "data": {
                    "answer": str(generation.get("answer") or ""),
                    "citations": generation.get("citations") or [],
                    "cost_seconds": round(cost, 2),
                },
                "trace_id": g.get("request_id"),
                "timestamp": datetime.now().isoformat(),
            }
            yield _sse("result", result_payload)
            yield _sse("done", {"success": True, "trace_id": g.get("request_id")})

        except Exception as e:
            app.logger.error(f"[{g.request_id}] 评估崩溃: {str(e)}\n{traceback.format_exc()}")
            error_payload = {
                "code": 500,
                "success": False,
                "message": "评估引擎内部故障，请检查数据完整性或 API 额度",
                "details": {"detail": str(e)},
                "trace_id": g.get("request_id"),
                "timestamp": datetime.now().isoformat(),
            }
            yield _sse("error", error_payload)
            yield _sse("done", {"success": False, "trace_id": g.get("request_id")})

    response = Response(generate(), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    response.headers["Connection"] = "keep-alive"
    return response


@app.route('/api/v1/risk/history', methods=['GET'])
def get_history():
    """
    获取历史评估记录 (支持网页端分页展示)
    """
    page = request.args.get('page', 1, type=int)
    size = request.args.get('size', 10, type=int)

    if "biz_risk_assessments" not in set(inspect(engine).get_table_names()):
        return ResponseFormatter.success({
            "list": [],
            "pagination": {"total": 0, "page": page, "size": size}
        })

    db = SessionLocal()
    try:
        # 查询评估历史表
        query = db.query(RiskAssessment).order_by(RiskAssessment.created_at.desc())
        total = query.count()
        records = query.offset((page - 1) * size).limit(size).all()

        history_list = []
        for r in records:
            history_list.append({
                "id": r.id,
                "summary": r.event_summary,
                "level": r.risk_level,
                "score": r.total_score,  # 基于 (得分 x 权重) 计算 [cite: 141]
                "time": r.created_at.strftime("%Y-%m-%d %H:%M")
            })

        return ResponseFormatter.success({
            "list": history_list,
            "pagination": {"total": total, "page": page, "size": size}
        })
    except Exception as e:
        app.logger.error(f"查询历史失败: {e}")
        return ResponseFormatter.error("数据库访问异常", 500)
    finally:
        db.close()

#===============================================
#功能二 内部行政文书起草
#===============================================

@app.route('/upload/document', methods=['POST'])  # 用户首次上传文档
def upload():
    def _sse(event: str, payload: dict) -> str:
        return f"event: {event}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

    file = request.files.get("file")
    if not file:
        return {
            "status": False,
            "message": "没有文件字段",
        }

    file_type = str(request.form.get("file_type") or "").strip()  # 1: 律师意见书, 2: 起诉状
    expected_template = str(request.form.get("expected_template") or "").strip() #1.案件报告  2.案件诉讼方案请示  3.聘请外部律师的请示

    if file.filename == "":
        return {
            "status": False,
            "message": "未选择文件",
        }

    if not file.filename.lower().endswith(".pdf"):
        return {
            "status": False,
            "message": "仅支持PDF文件",
        }

    try:
        os.makedirs("./uploads", exist_ok=True)
        saved_path = f"./uploads/{file.filename}"
        file.save(saved_path)

        print("文件上传成功，开始识别内容。。。")

        with pdfplumber.open(saved_path) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() or ""

        print("文件内容识别完毕")

        if file_type == "1":  # 用户传入的是律师意见书
            #调用大模型解析
            #parsed_elements = _parse_document_elements_with_llm(text, ELEMENT_PROMPT_LAWYER_OPINION)

            #创建映射表，为了把后续大模型解析出的结果的key映射成英文
            LAWYER_OPINION_FIELD_MAP = {
                "文书标题": "document_title",
                "文书类型": "document_type",
                "出具机构": "issuing_law_firm",
                "委托人": "client_name",
                "出具日期": "issue_date",
                "案件名称": "case_names",
                "案由": "case_causes",
                "法院/仲裁机构": "adjudicating_bodies",
                "案号": "case_numbers",
                "程序阶段": "procedure_stage",
                "当事人": "parties",
                "关键事实": "key_facts",
                "争议焦点": "dispute_issues",
                "适用法律": "applicable_laws",
                "律师分析": "lawyer_analysis",
                "律师结论": "lawyer_conclusion",
                "风险等级": "risk_level",
                "是否重大不利影响": "material_adverse_impact",
                "送达/核查材料": "reviewed_materials",
                "附件清单": "attachments",
            }

            def normalize_parsed_elements(raw_data: dict, field_map: dict) -> dict:
                normalized = {}
                for cn_key, db_key in field_map.items():
                    normalized[db_key] = raw_data.get(cn_key)
                return normalized

            def safe_str(value):
                if value is None:
                    return None
                value = str(value).strip()
                if not value:
                    return None
                return value

            def build_legal_opinion_record(content: str, parsed_elements: dict):
                return LegalOpinion(
                    content=content,
                    document_title=safe_str(parsed_elements.get("document_title")),
                    document_type=safe_str(parsed_elements.get("document_type")),
                    issuing_law_firm=safe_str(parsed_elements.get("issuing_law_firm")),
                    client_name=safe_str(parsed_elements.get("client_name")),
                    issue_date=safe_str(parsed_elements.get("issue_date")),
                    case_names=safe_str(parsed_elements.get("case_names")),
                    case_causes=safe_str(parsed_elements.get("case_causes")),
                    adjudicating_bodies=safe_str(parsed_elements.get("adjudicating_bodies")),
                    case_numbers=safe_str(parsed_elements.get("case_numbers")),
                    procedure_stage=safe_str(parsed_elements.get("procedure_stage")),
                    parties=safe_str(parsed_elements.get("parties")),
                    key_facts=safe_str(parsed_elements.get("key_facts")),
                    dispute_issues=safe_str(parsed_elements.get("dispute_issues")),
                    applicable_laws=safe_str(parsed_elements.get("applicable_laws")),
                    lawyer_analysis=safe_str(parsed_elements.get("lawyer_analysis")),
                    lawyer_conclusion=safe_str(parsed_elements.get("lawyer_conclusion")),
                    risk_level=safe_str(parsed_elements.get("risk_level")),
                    material_adverse_impact=safe_str(parsed_elements.get("material_adverse_impact")),
                    reviewed_materials=safe_str(parsed_elements.get("reviewed_materials")),
                    attachments=safe_str(parsed_elements.get("attachments")),
                )

            print("开始调用大模型解析。。。")
            #调用大模型解析源文件
            raw_parsed = _parse_document_elements_with_llm(text, ELEMENT_PROMPT_LAWYER_OPINION)
            print("大模型解析完毕")

            #把大模型解析的映射成英文
            parsed_elements = normalize_parsed_elements(raw_parsed, LAWYER_OPINION_FIELD_MAP)

            #将解析的要素与源文件入库
            session = SessionLocal()
            try:
                record = build_legal_opinion_record(text, parsed_elements)
                session.add(record)
                session.commit()
                row_id = record.id  # 获取刚刚写入数据库的数据行的id，为了后续利用该id查询该数据行
                session.refresh(record)
            finally:
                session.close()

        elif file_type == "2":  # 用户传入的是起诉状
            # 创建映射表，为了把后续大模型解析出的结果的key映射成英文
            COMPLAINT_FIELD_MAP = {
                "文档名称":"document_name",
                "文书类型":"document_type",
                "案由":"case_cause",
                "受理法院":"court_name",
                "具状日期":"filing_date",
                "原告名称列表":"plaintiff_names",
                "原告地址列表":"plaintiff_addresses",
                "原告法定代表人/负责人列表":"plaintiff_legal_representatives",
                "原告诉讼代理人列表":"plaintiff_attorneys",
                "被告名称列表": "defendant_names",
                "被告地址列表": "defendant_addresses",
                "被告法定代表人/负责人列表": "defendant_legal_representatives",
                "被告诉讼代理人列表": "defendant_attorneys",
                "第三人/其他相关主体列表": "third_party_entities",
                "诉讼请求全文": "claims_full_text",
                "诉讼请求分项列表": "claims_items",
                "本金/主债权金额": "principal_amount",
                "利息金额": "interest_amount",
                "逾期利息/罚息/违约金金额": "penalty_interest_amount",
                "复利金额": "compound_interest_amount",
                "律师费金额": "attorney_fees",
                "其他费用主张": "other_costs",
                "其他非金钱请求": "non_monetary_claims",
                "事实与理由全文": "facts_and_reasons",
                "案件涉及的主要合同/文件": "related_contracts_or_documents",
                "原告主张的履行行为": "plaintiff_performance_claims",
                "原告主张的违约/侵权事实": "plaintiff_breach_or_tort_claims",
                "原告主张的催收/通知/提前到期等前置行为": "plaintiff_pre_actions",
                "原告主张的关联被告责任依据": "joint_liability_basis",
                "起诉状引用法律依据": "legal_basis",
                "附件清单": "attachments",
            }

            def normalize_parsed_elements(raw_data: dict, field_map: dict) -> dict:
                normalized = {}
                for cn_key, db_key in field_map.items():
                    normalized[db_key] = raw_data.get(cn_key)
                return normalized

            def safe_str(value):
                if value is None:
                    return None
                value = str(value).strip()
                if not value:
                    return None
                return value

            def build_complaint_record(content: str, parsed_elements: dict):
                return Complaint(
                    # content=content,
                    # document_name=safe_str(parsed_elements.get("document_name")),
                    # court_name=safe_str(parsed_elements.get("court_name")),
                    # case_number=safe_str(parsed_elements.get("case_number")),
                    # plaintiffs=safe_str(parsed_elements.get("plaintiffs")),
                    # defendants=safe_str(parsed_elements.get("defendants")),
                    # third_parties=safe_str(parsed_elements.get("third_parties")),
                    # case_cause=safe_str(parsed_elements.get("case_cause")),
                    # claims_full_text=safe_str(parsed_elements.get("claims_full_text")),
                    # claims_items=safe_str(parsed_elements.get("claims_items")),
                    # principal_amount=safe_str(parsed_elements.get("principal_amount")),
                    # interest_claim=safe_str(parsed_elements.get("interest_claim")),
                    # liquidated_damages=safe_str(parsed_elements.get("liquidated_damages")),
                    # attorney_fees=safe_str(parsed_elements.get("attorney_fees")),
                    # preservation_fees=safe_str(parsed_elements.get("preservation_fees")),
                    # priority_repayment_claim=safe_str(parsed_elements.get("priority_repayment_claim")),
                    # joint_liability_claim=safe_str(parsed_elements.get("joint_liability_claim")),
                    # facts_and_reasons_full_text=safe_str(parsed_elements.get("facts_and_reasons_full_text")),
                    # key_contracts=safe_str(parsed_elements.get("key_contracts")),
                    # key_timestamps=safe_str(parsed_elements.get("key_timestamps")),
                    # current_case_progress=safe_str(parsed_elements.get("current_case_progress")),

                    content=content,
                    document_name=safe_str(parsed_elements.get("document_name")),
                    document_type=safe_str(parsed_elements.get("document_type")),
                    case_cause=safe_str(parsed_elements.get("case_cause")),
                    court_name=safe_str(parsed_elements.get("court_name")),
                    filing_date=safe_str(parsed_elements.get("filing_date")),
                    plaintiff_names=safe_str(parsed_elements.get("plaintiff_names")),
                    plaintiff_addresses=safe_str(parsed_elements.get("plaintiff_addresses")),
                    plaintiff_legal_representatives=safe_str(parsed_elements.get("plaintiff_legal_representatives")),
                    plaintiff_attorneys=safe_str(parsed_elements.get("plaintiff_attorneys")),
                    defendant_names=safe_str(parsed_elements.get("defendant_names")),
                    defendant_addresses=safe_str(parsed_elements.get("defendant_addresses")),
                    defendant_legal_representatives=safe_str(parsed_elements.get("defendant_legal_representatives")),
                    defendant_attorneys=safe_str(parsed_elements.get("defendant_attorneys")),
                    third_party_entities=safe_str(parsed_elements.get("third_party_entities")),
                    claims_full_text=safe_str(parsed_elements.get("claims_full_text")),
                    claims_items=safe_str(parsed_elements.get("claims_items")),
                    principal_amount=safe_str(parsed_elements.get("principal_amount")),
                    interest_amount=safe_str(parsed_elements.get("interest_amount")),
                    penalty_interest_amount=safe_str(parsed_elements.get("penalty_interest_amount")),
                    compound_interest_amount=safe_str(parsed_elements.get("compound_interest_amount")),
                    attorney_fees=safe_str(parsed_elements.get("attorney_fees")),
                    other_costs=safe_str(parsed_elements.get("other_costs")),
                    non_monetary_claims=safe_str(parsed_elements.get("non_monetary_claims")),
                    facts_and_reasons=safe_str(parsed_elements.get("facts_and_reasons")),
                    related_contracts_or_documents=safe_str(parsed_elements.get("related_contracts_or_documents")),
                    plaintiff_performance_claims=safe_str(parsed_elements.get("plaintiff_performance_claims")),
                    plaintiff_breach_or_tort_claims=safe_str(parsed_elements.get("plaintiff_breach_or_tort_claims")),
                    plaintiff_pre_actions=safe_str(parsed_elements.get("plaintiff_pre_actions")),
                    joint_liability_basis=safe_str(parsed_elements.get("joint_liability_basis")),
                    legal_basis=safe_str(parsed_elements.get("legal_basis")),
                    attachments=safe_str(parsed_elements.get("attachments")),
                )

            # 调用大模型解析源文件（system prompt + user prompt）
            complaint_system_prompt = """
            你是法律文书信息抽取助手。

你的任务是：从用户提供的“民事起诉状全文”中，抽取信息并输出为一个JSON对象，用于写入固定Excel表格。

你必须严格遵守以下规则：

一、抽取原则
1. 只做直接抽取，不做归纳，不做法律判断，不做风险分析。
2. 只能依据起诉状原文提取，不得补充原文没有的信息。
3. 不得生成“案件名称”之类原文未明确出现的归纳字段。
4. 若某字段原文未载明，输出空字符串 ""。
5. 不得猜测案号、立案日期、开庭时间、裁判结果等起诉状中没有的信息。
6. 不得把“可能”“应当”“推断”为确定事实。

二、字段规则
1. 必须严格按指定字段输出，不得增删字段，不得改字段名。
2. 多个主体、多个代理人、多个文件、多个请求等，统一用全角分号“；”连接。
3. “诉讼请求全文”“事实与理由全文”应保留原文主要内容，允许去除纯格式性空行，但不得改变意思。
4. “诉讼请求分项列表”应按起诉状中的请求顺序提取，统一写成“1.……；2.……；3.……”格式。
5. 金额字段仅填写能够直接对应字段含义的金额：
   - 本金/主债权金额
   - 利息金额
   - 逾期利息/罚息/违约金金额
   - 复利金额
   - 律师费金额
   如原文未单列，则填空字符串。
6. “其他费用主张”用于填写诉讼费、保全费、鉴定费、公告费、实现债权费用等非前述固定金额费用主张。
7. “其他非金钱请求”用于填写优先受偿、停止侵害、确认合同效力、解除合同、返还特定财产等非纯金钱请求。
8. “案件涉及的主要合同/文件”只提取起诉状正文中明确出现、对案件主张有支撑作用的合同、协议、函件、通知、担保文件等名称，不必拆编号、日期、金额。
9. “原告主张的履行行为”只提取原告自称已履行的行为，如已付款、已放款、已开证、已交付、已履约等。
10. “原告主张的违约/侵权事实”只提取原告诉称的对方违约、侵权、不履行、不足额履行等内容。
11. “原告主张的催收/通知/提前到期等前置行为”只提取原文中明确写明的催收、通知、提前到期通知、函告等。
12. “原告主张的关联被告责任依据”只提取原告诉请第二被告、第三被告或关联主体承担责任所依据的协议、承诺、函件、担保安排或其他明确依据。
13. “起诉状引用法律依据”只提取原文明确写出的法律、司法解释、规定名称；未写明则留空。
14. “附件清单”只提取起诉状末尾“附：”后的清单内容。

三、输出要求
1. 只输出一个JSON对象。
2. 不要输出markdown，不要输出代码块，不要解释，不要加前后说明。
3. JSON必须可解析。

            """
            complaint_user_prompt = """
            请从以下民事起诉状中抽取信息，并严格按照指定字段输出一个JSON对象。
【需要输出的字段】
文档名称
文书类型
案由
受理法院
具状日期
原告名称列表
原告地址列表
原告法定代表人/负责人列表
原告诉讼代理人列表
被告名称列表
被告地址列表
被告法定代表人/负责人列表
被告诉讼代理人列表
第三人/其他相关主体列表
诉讼请求全文
诉讼请求分项列表
本金/主债权金额
利息金额
逾期利息/罚息/违约金金额
复利金额
律师费金额
其他费用主张
其他非金钱请求
事实与理由全文
案件涉及的主要合同/文件
原告主张的履行行为
原告主张的违约/侵权事实
原告主张的催收/通知/提前到期等前置行为
原告主张的关联被告责任依据
起诉状引用法律依据
附件清单

【补充要求】
1. 文档名称：提取文档标题；如果正文没有独立标题，则可用用户传入的文件名。
2. 文书类型：如原文为“民事起诉状”，则填写“民事起诉状”。
3. 多值字段统一用全角分号“；”连接。
4. 缺失值统一填写“无”。
5. 不得输出字段以外的任何内容。
6. 只输出JSON对象。

【正文内容】
{{document_text}}

            """

            api_key = os.getenv("SILICONFLOW_API_KEY")
            if not api_key:
                raise ValueError("SILICONFLOW_API_KEY is not configured")

            client = OpenAI(
                api_key=api_key,
                base_url="https://api.deepseek.com/v1",
            )

            safe_text = _truncate_text(text or "")

            print("开始调用大模型解析。。。")

            response = client.chat.completions.create(
                model=ELEMENT_PARSER_MODEL,
                messages=[
                    {"role": "system", "content": complaint_system_prompt},
                    {"role": "user", "content": complaint_user_prompt.replace("{{document_text}}", safe_text)},
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
                max_tokens=3000,
            )

            content = ((response.choices[0].message.content if response and response.choices else "") or "").strip()
            if not content:
                raw_parsed = {}
            else:
                try:
                    raw_parsed = json.loads(content)
                except json.JSONDecodeError:
                    raw_parsed = {"raw_output": content}
            #print(type(raw_parsed))
            # print("raw keys:", list(raw_parsed.keys()))
            # print("missing:", set(COMPLAINT_FIELD_MAP.keys()) - set(raw_parsed.keys()))
            # print("extra:", set(raw_parsed.keys()) - set(COMPLAINT_FIELD_MAP.keys()))

            print("大模型解析完成")

            print(json.dumps(raw_parsed,ensure_ascii=False, indent=4))
            # 把大模型解析的映射成英文
            parsed_elements = normalize_parsed_elements(raw_parsed, COMPLAINT_FIELD_MAP)

            # 将解析的要素与源文件入库
            session = SessionLocal()
            try:
                record = build_complaint_record(text, parsed_elements)
                session.add(record)
                session.commit()
                row_id = record.id  #获取刚刚写入数据库的数据行的id，为了后续利用该id查询该数据行
                session.refresh(record)
            finally:
                session.close()

        else:
            return {
                "status": False,
                "message": "没传入正确的file_type",
            }
    except Exception as e:
        app.logger.error(f"[{g.get('request_id')}] 文档要素解析失败: {str(e)}\n{traceback.format_exc()}")
        return {
            "status": False,
            "message": f"文档要素解析失败: {str(e)}",
        }

    # 到这里，已经完成了文件解析与入库。接下来，读取数据库中已入库的数据并生成最终文本。
    template_user_prompt_map = {
        "1": """
        任务类型：案件报告

输出结构要求：
请输出一份《案件报告》，严格使用以下结构：

标题：
关于【原告名称列表】起诉【被告名称列表】等【案由】事项的案件报告

正文结构：
一、案件基本情况
二、案件主体情况
三、对方诉讼请求
四、起诉状记载的主要事实基础
五、起诉状所涉主要合同/文件
六、目前需重点关注的问题
七、后续建议事项

具体要求：
1. “案件基本情况”
- 写明文书类型、案由、受理法院、具状日期。
- 不补写案号、立案时间等未提供内容。

2. “案件主体情况”
- 列明原告、被告、第三人/其他相关主体。
- 若代理人已提取，可简要写明。

3. “对方诉讼请求”
- 优先依据“诉讼请求分项列表”组织。
- 同时结合金额字段规范呈现。
- 不自行汇总未提供总额。
- 如存在“其他非金钱请求”，单独列示。

4. “起诉状记载的主要事实基础”
- 仅根据“事实与理由全文”“原告主张的履行行为”“原告主张的违约/侵权事实”“原告主张的催收/通知/提前到期等前置行为”压缩表述。
- 必须使用“起诉状称”“原告主张”等表述。

5. “起诉状所涉主要合同/文件”
- 根据“案件涉及的主要合同/文件”和“原告主张的关联被告责任依据”表述。
- 如可区分，可分为“基础交易文件”和“责任连接文件”。

6. “目前需重点关注的问题”
- 只能写从起诉状文本和字段结果中可见、后续需进一步核查的问题。
- 不下结论，不作裁判式判断。

7. “后续建议事项”
- 用企业法务口径写3—5条即可。
- 建议应具体，如核查合同文件、核对金额构成、审查关联责任依据、准备应诉材料、必要时启动外聘律师程序等。

语言要求：
- 正式、专业、克制
- 不要口语化
- 不要输出分析过程
- 直接输出完整正文

        """,
        "2": """
        任务类型：案件诉讼方案请示

输出结构要求：
请输出一份《案件诉讼方案请示》，严格使用以下结构：

标题：
关于【原告名称列表】起诉【被告名称列表】等【案由】事项诉讼应对方案的请示

正文结构：
一、案件基本情况
二、拟采取的总体应对思路
三、拟采取的主要应对方案
   （一）程序层面安排
   （二）实体层面重点核查与应对方向
   （三）证据组织与材料核查安排
   （四）结果预案与后续安排
四、内部工作分工建议
五、需报请批准事项

具体要求：
1. “案件基本情况”
- 概述案由、法院、具状日期、原被告情况、主要请求金额和其他非金钱请求。
- 不补写未提供程序信息。

2. “拟采取的总体应对思路”
- 围绕已提取字段写一段总体思路。
- 典型表述应是“先核查基础合同和责任连接文件，再核对金额与费用构成，同时统筹程序应对”。

3. “拟采取的主要应对方案”
（1）程序层面安排
- 可写答辩准备、材料核查、程序节点跟踪、保全应对预判等。
- 但不得假设已经立案、保全、开庭，除非字段明确支持。

（2）实体层面重点核查与应对方向
- 围绕案件涉及的主要合同/文件、原告主张的违约/侵权事实、原告主张的关联被告责任依据、其他非金钱请求展开。
- 使用“重点核查”“重点审查”“重点关注”表述，不下实体结论。

（3）证据组织与材料核查安排
- 要写具体材料类型，如合同原件、履行凭证、催收通知、责任连接文件、费用依据等。
- 不要空泛。

（4）结果预案与后续安排
- 写“先完成基础核查，再视情况评估正式应诉、调解、和解、外聘律师等路径”。
- 不要直接建议放弃抗辩或直接和解。

4. “内部工作分工建议”
- 按法务、业务、财务、办公室/档案、关联公司等分工写。
- 分工要贴合字段内容。

5. “需报请批准事项”
- 必须用“同意……”句式列项。
- 可包括：同意按本方案推进、同意成立专项工作组、同意视情况启动外聘律师程序、同意根据案件进展另行报批专项事项等。

语言要求：
- 正式、务实、可执行
- 不要空话套话
- 不输出分析过程
- 直接输出完整正文

        """,
        "3": """
        任务类型：聘请外部律师的请示

输出结构要求：
请输出一份《聘请外部律师的请示》，严格使用以下结构：

标题：
关于聘请外部律师办理【原告名称列表】起诉【被告名称列表】等【案由】事项的请示

正文结构：
一、案件基本情况
二、拟聘请外部律师的必要性
三、拟委托外部律师的主要工作内容
四、拟选聘律师的基本条件
五、选聘方式及费用建议
六、拟请示事项

具体要求：
1. “案件基本情况”
- 写明案由、法院、具状日期、原被告情况、请求金额构成及其他非金钱请求。
- 如存在多个被告、第三人或关联责任依据，应简要点明。

2. “拟聘请外部律师的必要性”
- 必须结合字段具体写，不得套话。
- 可从以下方面展开：
  a. 金额规模较大
  b. 多主体责任边界较复杂
  c. 存在担保/增信/责任连接文件
  d. 存在其他非金钱请求
  e. 合同/文件较多
  f. 利息、逾期利息、复利、费用构成较复杂
- 如果字段不足，则写“从起诉状文本初步看”。

3. “拟委托外部律师的主要工作内容”
- 必须与已提取字段对应。
- 如审查主要合同/文件、核查关联被告责任依据、核对金额构成、参与应诉文书准备、参与非金钱请求应对等。

4. “拟选聘外部律师的基本条件”
- 从专业领域经验、同类案件经验、多主体责任争议处理能力、复杂金额争议处理能力、响应效率等方面写。
- 不指定具体律所。

5. “选聘方式及费用建议”
- 只写原则性建议。
- 例如：由法务部牵头，在公司制度框架内组织比选或询价。
- 不编造报价。

6. “拟请示事项”
- 必须用“同意……”句式列项。
- 可包括：同意启动外聘程序、同意法务部牵头比选、同意履行审批后签署委托协议、同意外部律师尽快介入案件准备等。

语言要求：
- 正式、审慎、务实
- 不要广告化
- 不要口语化
- 不输出分析过程
- 直接输出完整正文

        """,
    }
    if expected_template not in template_user_prompt_map:
        return {
            "status": False,
            "message": "expected_template 仅支持 1/2/3",
        }

    generation_shared_system_prompt = """
        你是企业法务部内部文书生成助手。

你的唯一主要输入来源，是用户提供的“起诉状统一抽取表”中的单行字段结果。你的任务不是还原起诉状原文，也不是代替法官或律师下结论，而是基于已提取字段，生成企业内部管理或审批使用的正式文书。

你必须严格遵守以下规则：

一、事实来源规则
1. 只能使用用户提供的字段值作为主要事实来源。
2. 不得虚构任何未在字段中出现的事实、日期、金额、合同名称、法院、主体、证据、程序节点。
3. 不得自行补充“案号”“立案日期”“开庭时间”“案件名称”等未提供信息。
4. 若字段为空、未提取、未载明或缺失，应使用“未提取”“起诉状未载明”“待进一步核实”等表达，不得擅自补足。

二、写作定位规则
1. 输出文书属于企业内部文书，而非诉讼文书、律师意见书、裁判文书、媒体稿件。
2. 文风必须正式、克制、专业、清楚。
3. 不得使用明显口语化、营销化、戏剧化表达。
4. 不得写成“原文摘抄堆砌”，应对字段内容进行规范化重述和组织。

三、边界规则
1. 可以基于字段做文书层面的组织、归并、压缩和正式表达，但不得改变原始含义。
2. 可以根据字段写“需重点核查”“建议关注”“后续应审查”等程序性、管理性表述。
3. 不得把起诉状中的单方主张直接写成已查明事实。
4. 涉及责任、风险、抗辩、金额构成等内容时，应使用“起诉状称”“原告主张”“从现有提取结果看”“仍待核实”等限定性表达。

四、多主体规则
1. 若存在多个原告、多个被告、第三人或其他相关主体，应尽量区分列示。
2. 若字段显示存在“原告主张的关联被告责任依据”，则在文书中应特别体现该点。
3. 若字段显示存在“其他非金钱请求”，则不得遗漏，应单独表述。

五、金额规则
1. 仅使用已提取的金额字段，不得自行推导总额。
2. 若金额字段为空，则不计算、不补写。
3. 金额展示时保持正式表达，按“本金/主债权金额、利息金额、逾期利息/罚息/违约金金额、复利金额、律师费金额、其他费用主张、其他非金钱请求”顺序优先组织。

六、输出规则
1. 必须严格按照用户指定的文书类型和输出结构写作。
2. 直接输出完整正文，不要输出分析过程，不要解释你如何生成。
3. 不要输出与正文无关的提示语、免责声明或模型说明。

    """
    generation_missing_value_rule_prompt = """
    缺失值统一规则：

1. 空字符串、null、NA、N/A、未提取、未载明，统一视为缺失。
2. 缺失字段在正文中的处理规则：
- 主体信息缺失：写“起诉状未载明”
- 金额字段缺失：不计算、不推导、不补写
- 文件名称缺失：写“相关文件仍待进一步核实”
- 责任依据缺失：写“相关责任基础仍待结合完整材料核实”
- 催收/通知/提前到期行为缺失：写“前置催收或通知情况起诉状未明确载明”
- 法律依据缺失：写“起诉状未明确列示具体法律依据”
3. 若“其他非金钱请求”为空，则对应文书中可不单列，但不得虚构。
4. 若“第三人/其他相关主体列表”为空，则不强行写第三人。
5. 若“被告诉讼代理人列表”为空，不要写“无”，而写“起诉状未载明”或直接不展开。

    """
    generation_output_length_control_prompt = """
    输出长度控制建议：

一、案件报告
- 控制在 800—1200 字
- 各部分均需有内容，但避免冗长复述
- “后续建议事项”控制在 3—5 条

二、案件诉讼方案请示
- 控制在 1000—1500 字
- 重点放在“主要应对方案”和“需报请批准事项”
- “需报请批准事项”控制在 3—5 项

三、聘请外部律师的请示
- 控制在 800—1200 字
- 重点放在“拟聘请外部律师的必要性”和“拟委托工作内容”
- “拟请示事项”控制在 3—4 项

    """
    generation_db_data_prompt = """
以下是“起诉状统一抽取表”的单行字段数据（JSON）：
{db_record_json}

写作要求：
1. 仅可使用以上字段作为事实来源，不得虚构。
2. 字段缺失时按缺失值规则处理。
3. 直接输出指定文书正文，不要附加解释。
    """.strip()
    template_user_prompt = template_user_prompt_map[expected_template]

    generation_session = SessionLocal()
    try:
        if file_type == "1":
            db_record = generation_session.query(LegalOpinion).filter(LegalOpinion.id == row_id).first()
        else:
            db_record = generation_session.query(Complaint).filter(Complaint.id == row_id).first()

        if not db_record:
            return {
                "status": False,
                "message": f"未找到已入库数据，row_id={row_id}",
            }

        db_record_payload = {
            column.name: (
                value.isoformat()
                if hasattr(value, "isoformat") and callable(value.isoformat)
                else value
            )
            for column in db_record.__table__.columns
            for value in [getattr(db_record, column.name)]
            if column.name != "content"
        }
    finally:
        generation_session.close()

    db_record_json = json.dumps(db_record_payload, ensure_ascii=False)

    @stream_with_context
    def generate_document_stream():
        try:
            api_key = os.getenv("SILICONFLOW_API_KEY")
            if not api_key:
                raise ValueError("SILICONFLOW_API_KEY is not configured")

            client = OpenAI(
                api_key=api_key,
                base_url="https://api.deepseek.com/v1",
            )

            yield _sse("status", {
                "stage": "generate",
                "message": "正在生成输出报告",
                "trace_id": g.get("request_id"),
            })

            generation_stream = client.chat.completions.create(
                model=ELEMENT_PARSER_MODEL,
                messages=[
                    {"role": "system", "content": generation_shared_system_prompt},
                    {"role": "user", "content": template_user_prompt},
                    {"role": "user", "content": generation_db_data_prompt.format(db_record_json=db_record_json)},
                    {"role": "user", "content": generation_missing_value_rule_prompt},
                    {"role": "user", "content": generation_output_length_control_prompt},
                ],
                temperature=0.2,
                max_tokens=3000,
                stream=True,
            )

            generated_parts = []
            streamed_chars = 0
            for chunk in generation_stream:
                delta = ""
                if chunk and getattr(chunk, "choices", None):
                    first_choice = chunk.choices[0] if chunk.choices else None
                    if first_choice and getattr(first_choice, "delta", None):
                        delta = getattr(first_choice.delta, "content", "") or ""
                if not delta:
                    continue

                generated_parts.append(delta)
                for ch in delta:
                    streamed_chars += 1
                    yield _sse("token", {
                        "delta": ch,
                        "chars": streamed_chars,
                        "trace_id": g.get("request_id"),
                    })

            generated_answer = "".join(generated_parts).strip()

            result_payload = {
                "status": True,
                "message": "文档要素解析与模板生成完成",
                "data": {
                    "file_type": file_type,
                    "expected_template": expected_template,
                    "row_id": row_id,
                    "parsed_elements": parsed_elements,
                    "generated_answer": generated_answer,
                },
            }
            yield _sse("result", result_payload)
            yield _sse("done", {"success": True, "trace_id": g.get("request_id")})
        except Exception as e:
            app.logger.error(f"[{g.get('request_id')}] 文档模板生成失败: {str(e)}\n{traceback.format_exc()}")
            yield _sse("error", {
                "status": False,
                "message": f"文档模板生成失败: {str(e)}",
                "trace_id": g.get("request_id"),
            })
            yield _sse("done", {"success": False, "trace_id": g.get("request_id")})

    response = Response(generate_document_stream(), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    response.headers["Connection"] = "keep-alive"
    return response


# =========================================================
# 5. 运行入口
# =========================================================
if __name__ == '__main__':

    srv_port = int(os.getenv("WEB_PORT", 5050))

    print("\n" + "*" * 60)
    print(f" 营商环境风险评估网页后端服务启动成功")
    print(f" 监听接口: http://127.0.0.1:{srv_port}/api/v1/risk/evaluate")
    print(f" 日志存储: {BASE_DIR}/logs/web_service.log")
    print("*" * 60 + "\n")

    # 开启 threaded=True 以支持网页端多用户并发访问
    app.run(host='0.0.0.0', port=srv_port, debug=False, threaded=True)
