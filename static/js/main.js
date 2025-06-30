// Main JavaScript for Name Screening System

document.addEventListener("DOMContentLoaded", function () {
  // Load data sources for selection
  loadDataSourceSelection();
  // Set up form submission
  const form = document.getElementById("screeningForm");
  form.addEventListener("submit", handleFormSubmit);
  // Set up system info toggle
  setupSystemInfoToggle();
});

function setupSystemInfoToggle() {
  const toggleButton = document.getElementById("systemInfoToggle");
  const systemInfoPanel = document.getElementById("systemInfoPanel");

  toggleButton.addEventListener("click", function () {
    const isVisible = systemInfoPanel.style.display !== "none";

    if (isVisible) {
      // Hide the panel
      systemInfoPanel.style.display = "none";
      toggleButton.innerHTML = '<i class="fas fa-cog me-2"></i>System Info';
      toggleButton.classList.remove("active");
    } else {
      // Show the panel
      systemInfoPanel.style.display = "block";
      systemInfoPanel.classList.add("fade-in-up");
      toggleButton.innerHTML =
        '<i class="fas fa-times me-2"></i>Hide System Info';
      toggleButton.classList.add("active");
    }
  });
}

async function loadDataSourceSelection() {
  const container = document.getElementById("dataSourceSelection");
  container.innerHTML =
    '<div class="text-center"><div class="spinner-border text-primary" role="status"></div></div>';
  const resp = await fetch("/api/list_sources");
  const data = await resp.json();
  let html = "";
  // CSV
  if (data.csv.length) {
    html += '<div class="mb-2"><strong>CSV Files</strong><br>';
    data.csv.forEach((f) => {
      html += `<div class="form-check"><input class="form-check-input ds-csv" type="checkbox" value="${f}" id="csv-${f}"><label class="form-check-label" for="csv-${f}">${f}</label></div>`;
    });
    html += "</div>";
  }
  // JSON
  if (data.json.length) {
    html += '<div class="mb-2"><strong>JSON Files</strong><br>';
    data.json.forEach((f) => {
      html += `<div class="form-check"><input class="form-check-input ds-json" type="checkbox" value="${f}" id="json-${f}"><label class="form-check-label" for="json-${f}">${f}</label></div>`;
    });
    html += "</div>";
  }
  // SQLite
  if (data.sqlite.length) {
    html += '<div class="mb-2"><strong>SQLite DBs</strong><br>';
    data.sqlite.forEach((db, idx) => {
      html += `<div class="mb-1"><input class="form-check-input ds-sqlite" type="checkbox" value="${db.path}" id="sqlite-${idx}"><label class="form-check-label" for="sqlite-${idx}">${db.path}</label>`;
      if (db.tables.length) {
        html += `<select class="form-select form-select-sm mt-1 ms-2 ds-sqlite-table" data-db="${db.path}" style="width:auto;display:inline-block;">`;
        db.tables.forEach((t) => {
          html += `<option value="${t}">${t}</option>`;
        });
        html += "</select>";
      }
      html += "</div>";
    });
    html += "</div>";
  }
  container.innerHTML = html;
  // Add change listeners to update schema info
  container.querySelectorAll("input,select").forEach((el) => {
    el.addEventListener("change", updateSchemaInfo);
  });
  updateSchemaInfo();
}

function getSelectedSources() {
  const container = document.getElementById("dataSourceSelection");
  const sources = [];
  // CSV
  container.querySelectorAll(".ds-csv:checked").forEach((el) => {
    sources.push({ type: "csv", name: el.value });
  });
  // JSON
  container.querySelectorAll(".ds-json:checked").forEach((el) => {
    sources.push({ type: "json", name: el.value });
  });
  // SQLite
  container.querySelectorAll(".ds-sqlite:checked").forEach((el) => {
    const dbPath = el.value;
    // Find the associated table dropdown
    const tableSel = el.parentElement.querySelector("select.ds-sqlite-table");
    if (tableSel) {
      sources.push({ type: "sqlite", name: dbPath, table: tableSel.value });
    }
  });
  return sources;
}

async function updateSchemaInfo() {
  const schemaDiv = document.getElementById("schemaInfo");
  const sources = getSelectedSources();
  if (!sources.length) {
    schemaDiv.innerHTML =
      '<div class="alert alert-info">No sources selected.</div>';
    return;
  }
  schemaDiv.innerHTML =
    '<div class="text-center"><div class="spinner-border text-primary" role="status"></div></div>';
  const resp = await fetch("/api/schema", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sources }),
  });
  const schemas = await resp.json();
  let html = "";
  schemas.forEach((s) => {
    html += `<div class="schema-card mb-2"><strong>${s.name}${
      s.table ? " (table: " + s.table + ")" : ""
    }</strong><br>Fields: <code>${s.fields.join(", ")}</code></div>`;
  });
  schemaDiv.innerHTML = html;
}

async function handleFormSubmit(event) {
  event.preventDefault();
  const resultsDiv = document.getElementById("results");
  const enrichedDiv = document.getElementById("enrichedProfile");
  const rawJsonDiv = document.getElementById("rawJsonResponse");
  resultsDiv.innerHTML = `<div class="text-center"><div class="spinner-border text-primary" role="status"></div><p class="mt-2">Screening in progress...</p></div>`;
  enrichedDiv.innerHTML = "";
  rawJsonDiv.textContent = "";
  // Get form data
  const formData = new FormData(event.target);
  const base_profile = {
    customer_id: formData.get("customer_id"),
    name: formData.get("name"),
    address: formData.get("address"),
    dob: formData.get("dob"),
  };
  const sources = getSelectedSources();
  if (!sources.length) {
    resultsDiv.innerHTML =
      '<div class="alert alert-warning">Please select at least one data source.</div>';
    return;
  }
  try {
    const response = await fetch("/api/match", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ base_profile, sources }),
    });
    const data = await response.json();
    displayRankedResults(data.ranked_results);
    enrichedDiv.innerHTML = `<strong>Enriched Profile:</strong><br><code>${JSON.stringify(
      data.enriched_profile,
      null,
      2
    )}</code>`;
    rawJsonDiv.textContent = JSON.stringify(data.raw_json, null, 2);
  } catch (error) {
    resultsDiv.innerHTML = `<div class="alert alert-danger">Error occurred while screening name. Please try again.</div>`;
    enrichedDiv.innerHTML = "";
    rawJsonDiv.textContent = "";
  }
}

function displayRankedResults(results) {
  const resultsDiv = document.getElementById("results");
  if (!results || !results.length) {
    resultsDiv.innerHTML =
      '<div class="alert alert-info">No matches found.</div>';
    return;
  }
  let html = '<div class="list-group">';
  results.forEach((r, i) => {
    html += `<div class="list-group-item"><strong>#${i + 1} [${
      r.source
    }]</strong> <span class="badge bg-primary ms-2">Score: ${(
      r.score * 100
    ).toFixed(1)}%</span><br><small>${
      r.reason
    }</small><br><code>${JSON.stringify(r.candidate, null, 2)}</code></div>`;
  });
  html += "</div>";
  resultsDiv.innerHTML = html;
}

// Display screening results
function displayResults(results) {
  const resultsDiv = document.getElementById("results");
  if (!results.matches || results.matches.length === 0) {
    resultsDiv.innerHTML =
      '<div class="alert alert-info">No matches found.</div>';
    return;
  }

  let html = `
        <div class="card mb-4">
            <div class="card-header bg-primary text-white">
                <h5 class="mb-0">Screening Results</h5>
            </div>
            <div class="card-body">
                <div class="row mb-3">
                    <div class="col-md-6">
                        <h6>Summary</h6>
                        <p>Total Matches: ${results.matches.length}</p>
                        <p>Overall Confidence: ${(
                          results.overall_confidence * 100
                        ).toFixed(1)}%</p>
                    </div>
                    <div class="col-md-6">
                        <h6>Search Strategy</h6>
                        <p>${results.search_strategy.join(" → ")}</p>
                    </div>
                </div>
                <div class="matches-container">
    `;

  results.matches.forEach((match, index) => {
    html += `
            <div class="match-card card mb-3">
                <div class="card-header d-flex justify-content-between align-items-center">
                    <h6 class="mb-0">Match ${index + 1}</h6>
                    <button class="btn btn-sm btn-outline-primary toggle-details" 
                            data-match-index="${index}">
                        Show Details
                    </button>
                </div>
                <div class="card-body">
                    <div class="match-summary">
                        <p><strong>Source:</strong> ${match.source}</p>
                        <p><strong>Confidence:</strong> ${(
                          match.confidence * 100
                        ).toFixed(1)}%</p>
                    </div>
                    <div class="match-details-container" style="display: none;">
                        ${generateMatchDetails(match)}
                        
                        <!-- Agent Analysis Section -->
                        <div class="agent-analysis mt-4">
                            <h6 class="text-primary">Agent Analysis</h6>
                            
                            <!-- Schema Detection Agent -->
                            <div class="schema-agent mb-3">
                                <h6 class="text-secondary">Schema Detection Analysis</h6>
                                <div class="row">
                                    <div class="col-md-6">
                                        <p><strong>Field Mappings:</strong></p>
                                        <ul class="list-unstyled">
                                            ${Object.entries(
                                              match.schema_analysis
                                                ?.field_mappings || {}
                                            )
                                              .map(
                                                ([key, value]) =>
                                                  `<li><small>${key} → ${value}</small></li>`
                                              )
                                              .join("")}
                                        </ul>
                                    </div>
                                    <div class="col-md-6">
                                        <p><strong>Schema Confidence:</strong> ${(
                                          match.schema_analysis?.confidence *
                                            100 || 0
                                        ).toFixed(1)}%</p>
                                    </div>
                                </div>
                            </div>
                            
                            <!-- Profile Matching Agent -->
                            <div class="profile-agent">
                                <h6 class="text-secondary">Profile Matching Analysis</h6>
                                <div class="row">
                                    <div class="col-md-6">
                                        <p><strong>Match Type:</strong> ${
                                          match.match_type
                                        }</p>
                                        <p><strong>Search Method:</strong> ${
                                          match.search_method
                                        }</p>
                                    </div>
                                    <div class="col-md-6">
                                        <p><strong>Cross Reference Confidence:</strong> ${(
                                          match.cross_reference_confidence * 100
                                        ).toFixed(1)}%</p>
                                        <p><strong>Similarity Score:</strong> ${(
                                          match.similarity_score * 100
                                        ).toFixed(1)}%</p>
                                    </div>
                                </div>
                                ${
                                  match.match_reasoning
                                    ? `
                                    <div class="mt-2">
                                        <p><strong>Match Reasoning:</strong></p>
                                        <p class="text-muted small">${match.match_reasoning}</p>
                                    </div>
                                `
                                    : ""
                                }
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
  });

  html += `
                </div>
            </div>
        </div>
    `;

  resultsDiv.innerHTML = html;

  // Add event listeners for the toggle buttons
  document.querySelectorAll(".toggle-details").forEach((button) => {
    button.addEventListener("click", function () {
      const matchIndex = this.getAttribute("data-match-index");
      const detailsContainer = this.closest(".match-card").querySelector(
        ".match-details-container"
      );
      const isVisible = detailsContainer.style.display !== "none";

      detailsContainer.style.display = isVisible ? "none" : "block";
      this.textContent = isVisible ? "Show Details" : "Hide Details";
    });
  });
}

function generateMatchDetails(match) {
  // Create sections for different types of information
  const basicInfo = [
    ["Match ID", match.match_id],
    ["Source", match.source],
    ["Search Method", match.search_method],
    ["Match Type", match.match_type],
    ["Matched Field", match.matched_field],
    ["Match Group", match.match_group],
    ["Group Size", match.group_size],
  ];

  const personalInfo = [];
  // Add all personal information fields that exist in the match
  if (match.customer_id) personalInfo.push(["Customer ID", match.customer_id]);
  if (match.full_name) personalInfo.push(["Full Name", match.full_name]);
  if (match.customer_name)
    personalInfo.push(["Customer Name", match.customer_name]);
  if (match.client_name) personalInfo.push(["Client Name", match.client_name]);
  if (match.display_name)
    personalInfo.push(["Display Name", match.display_name]);
  if (match.street_address)
    personalInfo.push(["Street Address", match.street_address]);
  if (match.address) personalInfo.push(["Address", match.address]);
  if (match.location) personalInfo.push(["Location", match.location]);
  if (match.date_of_birth)
    personalInfo.push(["Date of Birth", match.date_of_birth]);
  if (match.birth_date) personalInfo.push(["Birth Date", match.birth_date]);
  if (match.phone_number)
    personalInfo.push(["Phone Number", match.phone_number]);
  if (match.contact_info)
    personalInfo.push(["Contact Info", match.contact_info]);
  if (match.email) personalInfo.push(["Email", match.email]);
  if (match.registration_date)
    personalInfo.push(["Registration Date", match.registration_date]);

  const confidenceInfo = [
    ["Similarity Score", (match.similarity_score * 100).toFixed(1) + "%"],
    ["Confidence", (match.confidence * 100).toFixed(1) + "%"],
    [
      "Cross Reference Confidence",
      (match.cross_reference_confidence * 100).toFixed(1) + "%",
    ],
  ];

  // Generate HTML for each section
  const generateSection = (title, items) => `
        <div class="mb-3">
            <h6 class="text-primary">${title}</h6>
            <div class="row">
                ${items
                  .map(
                    ([key, value]) => `
                    <div class="col-md-6 mb-2">
                        <strong>${key}:</strong> 
                        <span class="text-muted">${value || "N/A"}</span>
                    </div>
                `
                  )
                  .join("")}
            </div>
        </div>
    `;

  return `
        <div class="match-details p-3 bg-light rounded">
            ${generateSection("Basic Information", basicInfo)}
            ${generateSection("Personal Information", personalInfo)}
            ${generateSection("Confidence Metrics", confidenceInfo)}
            ${
              match.match_reasoning
                ? `
                <div class="mt-3">
                    <h6 class="text-primary">Match Reasoning</h6>
                    <p class="text-muted small">${match.match_reasoning}</p>
                </div>
            `
                : ""
            }
        </div>
    `;
}

// Helper functions
function getConfidenceClass(confidence) {
  if (confidence >= 0.8) return "high-confidence";
  if (confidence >= 0.5) return "medium-confidence";
  return "low-confidence";
}

function getConfidenceBadgeClass(confidence) {
  if (confidence >= 0.8) return "bg-success";
  if (confidence >= 0.5) return "bg-warning";
  return "bg-danger";
}
