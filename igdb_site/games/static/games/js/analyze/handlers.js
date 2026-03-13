// games/static/games/js/analyze/handlers.js
/**
 * Обработчики событий для Game Analyzer
 */

import { showMessage } from './managers.js';
import {
    saveCurrentTab,
    saveScrollPosition,
    saveTabScrollPosition,
    clearTabStorage,
    clearTabScrollPositions
} from './utils.js';

import { AddKeywordHandler, DeleteKeywordHandler } from './keyword-handlers.js';

/* ============================================
   NORMALIZE KEYWORD HANDLER
   ============================================ */

class NormalizeKeywordHandler {
    constructor(analyzer) {
        this.analyzer = analyzer;
    }

    bind() {
        const normalizeButton = document.getElementById('normalize-keyword-button');
        const keywordInput = document.getElementById('new-keyword-input');

        if (!normalizeButton || !keywordInput) {
            console.warn('Normalize button or input not found');
            return;
        }

        normalizeButton.addEventListener('click', (e) => {
            e.preventDefault();
            this.handleNormalize();
        });

        keywordInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && e.ctrlKey) {
                e.preventDefault();
                this.handleNormalize();
            }
        });
    }

    handleNormalize() {
        const keywordInput = document.getElementById('new-keyword-input');
        const resultDiv = document.getElementById('normalization-result');
        const word = keywordInput ? keywordInput.value.trim() : '';

        if (!word) {
            this.analyzer.showMessage('Please enter a word to normalize', 'error');
            if (resultDiv) {
                resultDiv.innerHTML = '<span class="text-danger"><i class="bi bi-exclamation-circle me-1"></i>Enter a word first.</span>';
            }
            return;
        }

        const csrfToken = this.analyzer.getCSRFToken();
        if (!csrfToken) {
            this.analyzer.showMessage('Security token missing', 'error');
            return;
        }

        const normalizeButton = document.getElementById('normalize-keyword-button');
        const originalHTML = normalizeButton ? normalizeButton.innerHTML : '';
        if (normalizeButton) {
            normalizeButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Normalizing...';
            normalizeButton.disabled = true;
        }

        if (resultDiv) {
            resultDiv.innerHTML = '<span class="text-info"><i class="bi bi-hourglass-split me-1"></i>Normalizing...</span>';
        }

        fetch('/games/normalize-keyword/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken,
                'X-Requested-With': 'XMLHttpRequest'
            },
            body: JSON.stringify({ word: word })
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            if (data.success) {
                if (keywordInput) {
                    keywordInput.value = data.normalized;
                }

                if (resultDiv) {
                    resultDiv.innerHTML = `
                        <span class="text-success">
                            <i class="bi bi-check-circle me-1"></i>
                            <strong>${data.original}</strong> → <strong>${data.normalized}</strong>
                        </span>`;
                }
                this.analyzer.showMessage(data.message, 'success', 3000);
            } else {
                if (resultDiv) {
                    resultDiv.innerHTML = `<span class="text-danger"><i class="bi bi-x-circle me-1"></i>${data.message}</span>`;
                }
                this.analyzer.showMessage('❌ ' + data.message, 'error');
            }
        })
        .catch(error => {
            console.error('Error normalizing keyword:', error);
            if (resultDiv) {
                resultDiv.innerHTML = `<span class="text-danger"><i class="bi bi-x-circle me-1"></i>Error: ${error.message}</span>`;
            }
            this.analyzer.showMessage('❌ Error normalizing word: ' + error.message, 'error');
        })
        .finally(() => {
            if (normalizeButton) {
                normalizeButton.innerHTML = originalHTML;
                normalizeButton.disabled = false;
            }
        });
    }
}

/* ============================================
   TAB HANDLERS
   ============================================ */

export function bindTabSelect(analyzer) {
    if (!analyzer.elements.tabSelect) return;

    analyzer.elements.tabSelect.addEventListener('change', (e) => {
        const tabName = e.target.value;
        console.log(`Tab select changed to: ${tabName}`);

        if (analyzer.elements.tabInput) {
            analyzer.elements.tabInput.value = tabName;
        }

        analyzer.switchTabByName(tabName);

        saveCurrentTab(analyzer);
    });
}

export function bindTabScrollEvents(analyzer) {
    const tabPanes = document.querySelectorAll('.tab-pane');
    tabPanes.forEach(tabPane => {
        const textDisplayArea = tabPane.querySelector('.text-display-area');
        if (textDisplayArea) {
            let scrollTimeout;
            textDisplayArea.addEventListener('scroll', () => {
                clearTimeout(scrollTimeout);
                scrollTimeout = setTimeout(() => {
                    const tabName = tabPane.id;
                    saveTabScrollPosition(analyzer, tabName);
                }, 150);
            });
        }
    });
}

export function bindBootstrapTabs(analyzer) {
    if (!analyzer.elements.analyzeTabLinks || analyzer.elements.analyzeTabLinks.length === 0) return;

    analyzer.elements.analyzeTabLinks.forEach(link => {
        link.addEventListener('shown.bs.tab', (e) => {
            const href = e.target.getAttribute('href');
            if (href) {
                const tabName = href.substring(1);

                if (analyzer.currentTab && analyzer.currentTab !== tabName) {
                    saveTabScrollPosition(analyzer, analyzer.currentTab);
                }

                analyzer.onTabSwitch(tabName);

                setTimeout(() => {
                    analyzer.restoreTabScrollPosition(tabName);
                }, 100);

                saveCurrentTab(analyzer);
            }
        });

        link.addEventListener('click', (e) => {
            const href = e.target.getAttribute('href');
            if (href) {
                const tabName = href.substring(1);

                if (analyzer.currentTab && analyzer.currentTab !== tabName) {
                    saveTabScrollPosition(analyzer, analyzer.currentTab);
                }
            }
        });
    });
}

/* ============================================
   FORM HANDLERS
   ============================================ */

export function bindAnalyzeButton(analyzer) {
    if (!analyzer.elements.analyzeButton) {
        console.error('ANALYZE BUTTON NOT FOUND!');
        analyzer.showMessage('Error: Analyze button not found. Please refresh page.', 'error');
        return;
    }

    if (analyzer.analyzeClickHandler) {
        analyzer.elements.analyzeButton.removeEventListener('click', analyzer.analyzeClickHandler);
    }

    analyzer.analyzeClickHandler = async (e) => {
        console.log('=== ANALYZE BUTTON CLICKED (AJAX) ===');
        e.preventDefault();
        e.stopPropagation();

        if (!analyzer.elements.analyzeForm) {
            console.error('Form element not found');
            analyzer.showMessage('Error: Form not found', 'error');
            return;
        }

        analyzer.saveCurrentTab();
        analyzer.saveScrollPosition();
        analyzer.saveTabScrollPosition(analyzer.currentTab);
        analyzer.updateHiddenFields();

        const csrfToken = analyzer.getCSRFToken();
        if (!csrfToken) {
            console.error('CSRF token not found');
            analyzer.showMessage('Error: Security token missing. Please refresh page.', 'error');
            return;
        }

        const gameId = analyzer.options.gameId;
        const activeTab = analyzer.currentTab;

        if (!gameId) {
            analyzer.showMessage('Error: Game ID not found.', 'error');
            return;
        }

        const originalButtonHTML = analyzer.elements.analyzeButton.innerHTML;
        analyzer.elements.analyzeButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Analyzing...';
        analyzer.elements.analyzeButton.disabled = true;

        try {
            const response = await fetch(`/games/${gameId}/analyze/ajax/`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-CSRFToken': csrfToken,
                    'X-Requested-With': 'XMLHttpRequest'
                },
                body: JSON.stringify({
                    tab: activeTab
                })
            });

            const contentType = response.headers.get("content-type");
            if (!contentType || !contentType.includes("application/json")) {
                const text = await response.text();
                throw new Error(`Received non-JSON response: ${text.substring(0, 100)}`);
            }

            const data = await response.json();

            if (data.success) {
                const activePane = document.getElementById(activeTab);
                if (activePane) {
                    const textDisplayArea = activePane.querySelector('.text-display-area .text-content');
                    if (textDisplayArea) {
                        textDisplayArea.innerHTML = data.highlighted_html;
                    }
                }

                updateFoundItemsSidebar(analyzer, data.found_items, true);

                if (data.summary.found_count > 0) {
                    analyzer.showMessage(`✅ ${data.message}`, 'success', 5000);
                } else {
                    analyzer.showMessage(`ℹ️ ${data.message}`, 'info', 4000);
                }

                analyzer.hasUnsavedResults = true;

                setTimeout(() => {
                    analyzer.setupTooltips();
                }, 100);

            } else {
                analyzer.showMessage(`❌ ${data.error || 'Unknown error during analysis.'}`, 'error');
            }

        } catch (error) {
            console.error('Error during AJAX analysis:', error);
            analyzer.showMessage(`❌ Network or server error: ${error.message}`, 'error');
        } finally {
            analyzer.elements.analyzeButton.innerHTML = originalButtonHTML;
            analyzer.elements.analyzeButton.disabled = false;
        }
    };

    analyzer.elements.analyzeButton.addEventListener('click', analyzer.analyzeClickHandler);
}

function updateFoundItemsSidebar(analyzer, foundItemsData, hasUnsavedResults) {
    const sidebarContainer = document.querySelector('.col-lg-4');
    if (!sidebarContainer) return;

    let targetSidebarCard = null;
    const sidebarCards = sidebarContainer.querySelectorAll('.sidebar-card');

    sidebarCards.forEach(card => {
        const header = card.querySelector('.sidebar-card-header');
        if (header && header.innerText.includes('Found Elements')) {
            targetSidebarCard = card;
        }
    });

    if (!targetSidebarCard) {
        console.error('Could not locate Found Elements sidebar card.');
        return;
    }

    const cardBody = targetSidebarCard.querySelector('.sidebar-card-body');
    if (!cardBody) return;

    const hasAnyItems = foundItemsData && Object.keys(foundItemsData).some(key =>
        ['genres', 'themes', 'perspectives', 'game_modes', 'keywords'].includes(key) &&
        foundItemsData[key]?.length > 0
    );

    if (!hasAnyItems) {
        cardBody.innerHTML = `
            <div class="found-items-grid">
                <p class="text-muted text-center my-3">No elements found in this text.</p>
            </div>
        `;
        return;
    }

    let html = '<div class="found-items-grid" id="found-items-container">';
    const categoryNames = {
        'genres': 'Genres',
        'themes': 'Themes',
        'perspectives': 'Perspectives',
        'game_modes': 'Game Modes',
        'keywords': 'Keywords'
    };
    const categoryColors = {
        'genres': 'bg-success',
        'themes': 'bg-danger',
        'perspectives': 'bg-primary',
        'game_modes': 'bg-purple',
        'keywords': 'bg-warning text-dark'
    };

    for (const [catKey, catName] of Object.entries(categoryNames)) {
        const items = foundItemsData[catKey];
        if (items && items.length > 0) {
            const newCount = foundItemsData[`${catKey}_new_count`] || 0;
            html += `
                <div class="found-items-category" data-category="${catKey}">
                    <h6>
                        ${catName} (${items.length})
                        ${newCount > 0 ? `<span class="badge bg-success ms-2" data-bs-toggle="tooltip" title="New elements (not saved yet)">${newCount} new</span>` : ''}
                    </h6>
                    <div class="found-items-list">`;

            items.forEach(item => {
                const badgeClass = item.is_new ? categoryColors[catKey] : 'bg-secondary';
                html += `<span class="badge ${badgeClass} found-item-badge"
                              data-name="${item.name}"
                              data-bs-toggle="tooltip"
                              title="${item.is_new ? 'New (not saved yet) - Click to scroll to highlight' : 'Already exists in game - Click to scroll to highlight'}">
                            ${item.name}
                            ${item.is_new ? '<i class="bi bi-plus-circle ms-1"></i>' : '<i class="bi bi-check-circle ms-1"></i>'}
                        </span>`;
            });

            html += `</div></div>`;
        }
    }

    html += '</div>';

    if (hasUnsavedResults) {
        html += `
            <div class="mt-3 pt-3 border-top border-secondary">
                <div class="alert alert-info mb-0 py-2">
                    <small>
                        <i class="bi bi-info-circle me-1"></i>
                        Found elements are displayed but not saved to database yet.
                        Click "Save Results" button to save.
                    </small>
                </div>
            </div>`;
    }

    cardBody.innerHTML = html;

    setTimeout(() => {
        if (typeof bindFoundItemsClicks === 'function') {
            bindFoundItemsClicks(analyzer);
        }
        analyzer.setupTooltips();
    }, 50);
}

export function bindSaveButton(analyzer) {
    if (!analyzer.elements.saveButton) return;

    const newButton = analyzer.elements.saveButton.cloneNode(true);
    analyzer.elements.saveButton.parentNode.replaceChild(newButton, analyzer.elements.saveButton);
    analyzer.elements.saveButton = newButton;

    analyzer.elements.saveButton.addEventListener('click', (e) => {
        e.preventDefault();

        saveCurrentTab(analyzer);
        saveScrollPosition(analyzer);
        analyzer.saveTabScrollPosition(analyzer.currentTab);
        handleSaveResults(analyzer, e);
    });
}

export function bindAddKeywordButton(analyzer) {
    console.log('Binding AJAX add keyword handler...');
    const addHandler = new AddKeywordHandler(analyzer);
    addHandler.bind();
}

export function bindDeleteKeywordButton(analyzer) {
    console.log('Binding AJAX delete keyword handler...');
    const deleteHandler = new DeleteKeywordHandler(analyzer);
    deleteHandler.bind();
}

export function bindNormalizeKeywordButton(analyzer) {
    console.log('Binding normalize keyword button...');
    const normalizeHandler = new NormalizeKeywordHandler(analyzer);
    normalizeHandler.bind();
}

export function bindClearResultsButton(analyzer) {
    if (!analyzer.elements.clearResultsBtn) return;

    const originalButton = analyzer.elements.clearResultsBtn;
    const newButton = originalButton.cloneNode(true);
    originalButton.parentNode.replaceChild(newButton, originalButton);
    const clearBtn = newButton;

    clearBtn.addEventListener('click', async (e) => {
        e.preventDefault();
        e.stopPropagation();

        if (!confirm('Are you sure you want to clear unsaved analysis results?\n\nThis action cannot be undone.')) {
            console.log('Clear cancelled by user');
            return;
        }

        saveCurrentTab(analyzer);
        saveScrollPosition(analyzer);
        analyzer.saveTabScrollPosition(analyzer.currentTab);

        const originalHTML = clearBtn.innerHTML;
        clearBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Clearing...';
        clearBtn.disabled = true;

        const clearUrl = clearBtn.getAttribute('href');

        if (!clearUrl) {
            console.error('Clear URL not found');
            analyzer.showMessage('Error: Clear URL not found', 'error');
            resetClearButton(clearBtn, originalHTML);
            return;
        }

        try {
            const response = await fetch(clearUrl, {
                method: 'GET',
                headers: {
                    'X-Requested-With': 'XMLHttpRequest',
                    'Accept': 'application/json',
                },
                credentials: 'same-origin'
            });

            if (response.ok) {
                const data = await response.json();

                if (data.success) {
                    analyzer.showMessage('✅ ' + data.message, 'success');

                    if (data.redirect_url) {
                        setTimeout(() => {
                            window.location.href = data.redirect_url;
                        }, 1500);
                    } else {
                        setTimeout(() => {
                            window.location.reload();
                        }, 1500);
                    }
                } else {
                    throw new Error(data.message || 'Clear error');
                }
            } else {
                throw new Error(`HTTP error: ${response.status}`);
            }
        } catch (error) {
            console.error('Error clearing results:', error);
            analyzer.showMessage(`❌ Error clearing results: ${error.message}`, 'error');
            resetClearButton(clearBtn, originalHTML);
        }
    });
}

export function bindBackToGameButton(analyzer) {
    if (!analyzer.elements.backToGameBtn) return;

    analyzer.elements.backToGameBtn.addEventListener('click', (e) => {
        clearTabStorage(analyzer);
        clearTabScrollPositions(analyzer);
        saveScrollPosition(analyzer);
    });
}

export function bindScrollToTop(analyzer) {
    if (!analyzer.elements.scrollToTopBtn) return;

    analyzer.elements.scrollToTopBtn.addEventListener('click', () => analyzer.scrollToTop());
    window.addEventListener('scroll', () => analyzer.handleScroll());
}

export function bindFoundItemsClicks(analyzer) {
    document.addEventListener('click', (e) => {
        const target = e.target;
        if (!target || !target.closest) return;

        const foundItem = target.closest('.found-item-badge');
        if (foundItem) {
            e.preventDefault();
            const elementName = foundItem.dataset.name;
            if (elementName) {
                const previousSelected = document.querySelectorAll('.found-item-badge.selected');
                previousSelected.forEach(el => {
                    el.classList.remove('selected');
                });

                foundItem.classList.add('selected');

                analyzer.scrollToHighlight(elementName);
            }
        }
    });
}

export function setupTooltips(analyzer) {
    if (analyzer && typeof analyzer.setupTooltips === 'function') {
        analyzer.setupTooltips();
    }
}

export function setupMultiCriteriaTooltips(analyzer) {
    document.addEventListener('mouseenter', (e) => {
        const target = e.target;
        if (!target || !target.closest) return;

        const multiElement = target.closest('.highlight-multi');
        if (multiElement) {
            showMultiCriteriaTooltip(analyzer, multiElement, e);
        }
    }, true);

    document.addEventListener('mouseleave', (e) => {
        const target = e.target;
        if (!target || !target.closest) return;

        const multiElement = target.closest('.highlight-multi');
        if (multiElement) {
            hideMultiCriteriaTooltip(analyzer);
        }
    }, true);
}

export function setupHighlightEvents(analyzer) {
    document.addEventListener('mouseenter', (e) => {
        const target = e.target;
        if (!target || !target.closest) return;

        const highlightElement = target.closest('.highlight-genre, .highlight-theme, .highlight-perspective, .highlight-game_mode, .highlight-keyword, .highlight-multi');

        if (highlightElement) {
            handleHighlightHover(analyzer, highlightElement, true);
        }
    }, true);

    document.addEventListener('mouseleave', (e) => {
        const target = e.target;
        if (!target || !target.closest) return;

        const highlightElement = target.closest('.highlight-genre, .highlight-theme, .highlight-perspective, .highlight-game_mode, .highlight-keyword, .highlight-multi');

        if (highlightElement) {
            handleHighlightHover(analyzer, highlightElement, false);
        }
    }, true);
}

/* ============================================
   HELPER FUNCTIONS
   ============================================ */

function resetClearButton(button, originalHTML) {
    if (button) {
        button.innerHTML = originalHTML;
        button.disabled = false;
    }
}

function handleSaveResults(analyzer, e) {
    e.preventDefault();

    if (!confirm('Are you sure you want to save the analysis results to the game database?\n\nThis will add new elements (genres, themes, keywords, etc.) to the game.')) {
        console.log('Save cancelled by user');
        return;
    }

    if (!analyzer.elements.analyzeForm) {
        analyzer.showMessage('Error: Form not found. Cannot save results.', 'error');
        return;
    }

    const saveButton = analyzer.elements.analyzeForm.querySelector('button[name="save_results"]');
    if (saveButton) {
        const originalHTML = saveButton.innerHTML;
        saveButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Saving...';
        saveButton.disabled = true;
        saveButton.dataset.originalHTML = originalHTML;
    }

    analyzer.showMessage('Saving results to database...', 'info');

    const saveInput = document.createElement('input');
    saveInput.type = 'hidden';
    saveInput.name = 'save_results';
    saveInput.value = 'true';
    analyzer.elements.analyzeForm.appendChild(saveInput);

    setTimeout(() => {
        try {
            analyzer.elements.analyzeForm.submit();
        } catch (error) {
            analyzer.showMessage('❌ Error submitting form: ' + error.message, 'error');

            if (saveButton && saveButton.dataset.originalHTML) {
                saveButton.innerHTML = saveButton.dataset.originalHTML;
                saveButton.disabled = false;
            }
        }
    }, 100);
}

function handleHighlightHover(analyzer, element, isEntering) {
    if (isEntering) {
        element.classList.add('highlight-hover');
    } else {
        element.classList.remove('highlight-hover');
    }
}

function showMultiCriteriaTooltip(analyzer, element, event) {
    hideMultiCriteriaTooltip(analyzer);

    const names = element.dataset.elementNames ? element.dataset.elementNames.split(',') : [];
    const categories = element.dataset.categories ? element.dataset.categories.split(',') : [];

    if (names.length === 0 || categories.length === 0 || names.length !== categories.length) {
        return;
    }

    const tooltip = document.createElement('div');
    tooltip.className = 'multi-criteria-tooltip';
    tooltip.style.cssText = `
        position: fixed;
        background: linear-gradient(135deg, #2c3e50, #4a5568);
        color: white;
        padding: 12px;
        border-radius: 8px;
        border: 1px solid #4a5568;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        z-index: 9999;
        max-width: 350px;
        font-size: 0.9rem;
        pointer-events: none;
        animation: tooltipFadeIn 0.2s ease-out;
    `;

    const title = document.createElement('strong');
    title.textContent = `Multiple Criteria Found (${names.length})`;
    title.style.cssText = `
        display: block;
        color: #ff6b35;
        margin-bottom: 8px;
        font-size: 0.95rem;
        border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        padding-bottom: 4px;
    `;
    tooltip.appendChild(title);

    const list = document.createElement('div');
    list.className = 'multi-criteria-list';

    const categoryNames = {
        'genres': 'Genre',
        'themes': 'Theme',
        'perspectives': 'Perspective',
        'game_modes': 'Game Mode',
        'keywords': 'Keyword'
    };

    const colors = {
        'genres': '#28a745',
        'themes': '#dc3545',
        'perspectives': '#007bff',
        'game_modes': '#6f42c1',
        'keywords': '#ffc107'
    };

    for (let i = 0; i < names.length; i++) {
        const category = categories[i];
        const name = names[i];
        const categoryDisplay = categoryNames[category] || category;
        const color = colors[category] || '#6c757d';

        const item = document.createElement('div');
        item.className = 'criteria-item';
        item.style.cssText = `
            display: flex;
            align-items: center;
            margin-bottom: 6px;
            padding: 4px 6px;
            border-radius: 4px;
            background: rgba(255, 255, 255, 0.05);
        `;

        const colorDot = document.createElement('span');
        colorDot.className = 'criteria-color';
        colorDot.style.cssText = `
            width: 10px;
            height: 10px;
            border-radius: 50%;
            margin-right: 10px;
            background: ${color};
            flex-shrink: 0;
        `;

        const text = document.createElement('span');
        text.innerHTML = `<strong>${categoryDisplay}:</strong> ${escapeHtml(name)}`;
        text.style.cssText = `
            flex-grow: 1;
            font-size: 0.85rem;
            line-height: 1.3;
        `;

        item.appendChild(colorDot);
        item.appendChild(text);
        list.appendChild(item);
    }

    tooltip.appendChild(list);

    document.body.appendChild(tooltip);
    analyzer.currentMultiTooltip = tooltip;

    let pageX, pageY;

    if (event && typeof event.pageX === 'number') {
        pageX = event.pageX;
        pageY = event.pageY;
    } else if (event && event.touches && event.touches.length > 0) {
        pageX = event.touches[0].pageX;
        pageY = event.touches[0].pageY;
    } else if (event && typeof event.clientX === 'number') {
        pageX = event.clientX + window.scrollX;
        pageY = event.clientY + window.scrollY;
    } else {
        const rect = element.getBoundingClientRect();
        pageX = rect.left + rect.width / 2 + window.scrollX;
        pageY = rect.top + rect.height / 2 + window.scrollY;
    }

    positionMultiCriteriaTooltip(analyzer, tooltip, pageX, pageY);
}

function positionMultiCriteriaTooltip(analyzer, tooltip, pageX, pageY) {
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    const tooltipWidth = tooltip.offsetWidth;
    const tooltipHeight = tooltip.offsetHeight;

    let left = pageX + 10;
    let top = pageY + 10;

    if (left + tooltipWidth > viewportWidth - 10) {
        left = pageX - tooltipWidth - 10;
    }

    if (top + tooltipHeight > viewportHeight - 10) {
        top = pageY - tooltipHeight - 10;
    }

    if (top < 10) {
        top = 10;
    }

    tooltip.style.left = left + 'px';
    tooltip.style.top = top + 'px';
}

function hideMultiCriteriaTooltip(analyzer) {
    if (analyzer.currentMultiTooltip && analyzer.currentMultiTooltip.parentNode) {
        analyzer.currentMultiTooltip.parentNode.removeChild(analyzer.currentMultiTooltip);
        analyzer.currentMultiTooltip = null;
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}