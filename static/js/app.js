const analyzeBtn = document.getElementById("analyze-btn");
const clearBtn = document.getElementById("clear-btn");
const sqlInput = document.getElementById("sql-input");
const traceOutput = document.getElementById("trace-output");
const lineageOutput = document.getElementById("lineage-output");

const showMessage = (element, message) => {
  element.textContent = message;
};

const renderResults = (data) => {
  traceOutput.textContent = JSON.stringify(data.traceability, null, 2);
  lineageOutput.textContent = JSON.stringify(data.lineage, null, 2);
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
    const response = await fetch("/analyze", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ sql }),
    });
    const data = await response.json();
    renderResults(data);
  } catch (error) {
    showMessage(traceOutput, "Error al analizar la entrada.");
    showMessage(lineageOutput, "No se pudo generar el linaje.");
  }
});

clearBtn.addEventListener("click", () => {
  sqlInput.value = "";
  showMessage(traceOutput, "Esperando análisis...");
  showMessage(lineageOutput, "Esperando análisis...");
});
