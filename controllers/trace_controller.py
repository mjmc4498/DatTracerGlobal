from flask import Blueprint, jsonify, render_template, request
from models.sql_trace_model import SqlTraceModel

trace_bp = Blueprint("trace", __name__)


@trace_bp.get("/")
def index():
    return render_template("index.html")


@trace_bp.post("/analyze")
def analyze_sql():
    payload = request.get_json(silent=True) or {}
    sql_text = payload.get("sql", "")
    analyzer = SqlTraceModel()
    results = analyzer.analyze(sql_text)
    return jsonify(results)
