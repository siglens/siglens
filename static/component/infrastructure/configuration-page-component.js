/*
 * Copyright (c) 2021-2024 SigScalr, Inc.
 *
 * This file is part of SigLens Observability Solution
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
class ConfigurationPage {
    constructor(container) {
        this.container = container;
        this.clusterName = 'my-cluster';
        this.namespace = 'default';
        this.render();
        this.initializeListeners();
    }

    generateCommand() {
        const clusterValue = this.clusterName.trim() || '""';
        const namespaceValue = this.namespace.trim() || '""';

        return `
helm repo add siglens https://siglens.github.io/helm-charts &&
helm repo update &&
helm upgrade --install --version ^2 --atomic --timeout 300s siglens-k8s-monitoring siglens/k8s-monitoring \\
--namespace ${namespaceValue} --create-namespace --values << EOF
cluster:
  name: ${clusterValue}
destinations:
  - name: siglens-cloud-metrics
    type: prometheus
    url: SIGLENS_CLOUD_METRICS_URL/api/prom/push
EOF`.trimEnd();
    }

    render() {
        this.container.innerHTML = `
        <div class="config-container">
          <div class="nav-tabs">
            <button class="tab active" data-tab="cluster">Cluster configuration</button>
          </div>

          <div class="content">
            <section class="section" id="prerequisites">
              <h2 class="section-title">1. Before you begin</h2>
              <p>You need the following available on your local machine:</p>
              <div class="list">
                <div class="list-item">
                  <span>The</span>
                  <code class="code">kubectl</code>
                  <span>command-line tool</span>
                </div>
                <div class="list-item">
                  <span>The</span>
                  <code class="code">helm</code>
                  <span>command-line tool for managing Helm charts</span>
                </div>
              </div>
            </section>

            <section class="section" id="cluster-info">
              <h2 class="section-title">2. Select features and enter cluster information</h2>
              <div>
                <label class="input-label">Cluster name:</label>
                <input type="text" class="input-field" id="clusterName" value="${this.clusterName}">
              </div>
              <div class="mt-3">
                <label class="input-label">Namespace:</label>
                <input type="text" class="input-field" id="namespace" value="${this.namespace}">
              </div>
            </section>

            <section class="section" id="deployment">
              <h2 class="section-title">3. Deploy Monitoring Resources on the Cluster</h2>
              <div class="code-block" id="commandBlock">
                ${this.generateCommand()}
              </div>
            </section>

            <section class="section" id="instrumentation">
                <h2 class="section-title">4. Configure application instrumentation</h2>
                <p>After the Helm chart is deployed, configure your application instrumentation to send telemetry data using these addresses:</p>
            
                <div class="endpoint">
                    <div class="endpoint-label">OTLP/gRPC endpoint:</div>
                    <div class="code-block">http://siglens-k8s-monitoring-alloy-receiver.${this.namespace}.svc.cluster.local:4317</div>
                </div>
                
                <div class="endpoint">
                    <div class="endpoint-label">OTLP/HTTP endpoint:</div>
                    <div class="code-block">http://siglens-k8s-monitoring-alloy-receiver.${this.namespace}.svc.cluster.local:4318</div>
                </div>
                
                <div class="endpoint">
                    <div class="endpoint-label">Zipkin endpoint:</div>
                    <div class="code-block">siglens-k8s-monitoring-alloy-receiver.${this.namespace}.svc.cluster.local:9411</div>
                </div>
            </section>

            <section class="section" id="explore">
              <h2 class="section-title">5. Explore your data</h2>
              <div>
                <button class="btn btn-secondary">See cluster status</button>
              </div>
            </section>
          </div>
        </div>
      `;
    }

    updateEndpoints() {
        const endpoints = document.querySelectorAll('.endpoint .code-block');
        const namespaceValue = this.namespace.trim() || '""';
        
        endpoints.forEach((endpoint) => {
            const baseUrl = 'siglens-k8s-monitoring-alloy-receiver';
            const port = endpoint.textContent.split(':').pop();
            const prefix = endpoint.textContent.startsWith('http') ? 'http://' : '';
            
            endpoint.textContent = `${prefix}${baseUrl}.${namespaceValue}.svc.cluster.local:${port}`;
        });
    }

    updateCommand() {
        const commandBlock = document.getElementById('commandBlock');
        commandBlock.innerHTML = this.generateCommand();
    }

    initializeListeners() {
        const clusterInput = document.getElementById('clusterName');
        const namespaceInput = document.getElementById('namespace');

        clusterInput.addEventListener('input', (e) => {
            this.clusterName = e.target.value.trim();
            this.updateCommand();
        });

        namespaceInput.addEventListener('input', (e) => {
            this.namespace = e.target.value.trim();
            this.updateCommand();
            this.updateEndpoints();
        });
    }
}
