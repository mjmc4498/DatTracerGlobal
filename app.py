from flask import Flask
from controllers.trace_controller import trace_bp


def create_app():
    app = Flask(__name__)
    app.register_blueprint(trace_bp)
    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=True)
