const analyzeBtn = document.getElementById("analyze-btn");
const clearBtn = document.getElementById("clear-btn");
const sqlInput = document.getElementById("sql-input");
const traceOutput = document.getElementById("trace-output");
const lineageOutput = document.getElementById("lineage-output");
const apiBase = document.body?.dataset.apiBase?.trim();

const showMessage = (element, message) => {
  element.textContent = message;
};

const renderResults = (data) => {
  traceOutput.textContent = JSON.stringify(data.traceability, null, 2);
  lineageOutput.textContent = JSON.stringify(data.lineage, null, 2);
};

const buildApiUrl = (path) => {
  if (!apiBase) {
    return path;
  }
  return `${apiBase.replace(/\\/$/, "")}/${path}`;
};

analyzeBtn.addEventListener("click", async () => {
  const sql = sqlInput.value.trim();
  if (!sql) {
    showMessage(traceOutput, "Ingresa sentencias SQL para analizar.");
    showMessage(lineageOutput, "El linaje se mostrará aquí.");
    return;
  }

  showMessage(traceOutput, "Analizando...");
  showMessage(lineageOutput, "Analizando...");

  try {
    const response = await fetch(buildApiUrl("analyze"), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ sql }),
    });
    const data = await response.json();
    renderResults(data);
  } catch (error) {
    const message =
      apiBase || !window.location.hostname.endsWith("github.io")
        ? "Error al analizar la entrada."
        : "Configura data-api-base con la URL de tu API para analizar en GitHub Pages.";
    showMessage(traceOutput, message);
    showMessage(
      lineageOutput,
      "No se pudo generar el linaje. Verifica que tu API esté disponible."
    );
  }
});

clearBtn.addEventListener("click", () => {
  sqlInput.value = "";
  showMessage(traceOutput, "Esperando análisis...");
  showMessage(lineageOutput, "Esperando análisis...");
});
