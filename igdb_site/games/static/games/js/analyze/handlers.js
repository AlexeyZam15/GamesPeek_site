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

/* ============================================
   DELETE KEYWORD HANDLER
   ============================================ */

class DeleteKeywordHandler {
    constructor(analyzer) {
        this.analyzer = analyzer;
        this.gameId = this._getGameId();
    }

    _getGameId() {
        const gameIdElement = document.getElementById('game-id');
        if (gameIdElement && gameIdElement.value) {
            return gameIdElement.value;
        }

        const urlMatch = window.location.pathname.match(/\/games\/(\d+)\/analyze/);
        if (urlMatch && urlMatch[1]) {
            return urlMatch[1];
        }

        const gameIdInput = document.querySelector('input[name="game_id"], input[name="game-id"]');
        if (gameIdInput && gameIdInput.value) {
            return gameIdInput.value;
        }

        console.error('Game ID not found on page');
        return null;
    }

    bind() {
        const deleteButton = document.getElementById('delete-keyword-button');
        const keywordInput = document.getElementById('new-keyword-input');

        if (!deleteButton) return;

        if (!this.gameId) {
            console.error('Cannot bind delete button: Game ID not available');
            deleteButton.disabled = true;
            deleteButton.title = 'Game ID not available';
            return;
        }

        deleteButton.addEventListener('click', (e) => {
            e.preventDefault();
            this.handleDeleteKeyword();
        });

        if (keywordInput) {
            keywordInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' && e.shiftKey) {
                    e.preventDefault();
                    this.handleDeleteKeyword();
                }
            });
        }
    }

    handleDeleteKeyword() {
        const keywordInput = document.getElementById('new-keyword-input');
        const keyword = keywordInput ? keywordInput.value.trim() : '';

        if (!keyword) {
            this.analyzer.showMessage('Please enter a keyword to delete', 'error');
            return;
        }

        if (!confirm(`Are you sure you want to delete keyword "${keyword}"?\n\nThis will remove the keyword from all games and delete it from the database.`)) {
            return;
        }

        const csrfToken = this.getCSRFToken();
        if (!csrfToken) {
            this.analyzer.showMessage('Security token missing', 'error');
            return;
        }

        if (!this.gameId) {
            this.analyzer.showMessage('Game ID not found. Please refresh the page.', 'error');
            return;
        }

        const deleteButton = document.getElementById('delete-keyword-button');
        const originalHTML = deleteButton ? deleteButton.innerHTML : '';
        if (deleteButton) {
            deleteButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Deleting...';
            deleteButton.disabled = true;
        }

        const currentTab = this.analyzer.currentTab || 'summary';

        fetch(`/games/${this.gameId}/analyze/delete-keyword/`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken,
                'X-Requested-With': 'XMLHttpRequest'
            },
            body: JSON.stringify({
                keyword: keyword,
                tab: currentTab,
                auto_analyze: true
            })
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            if (data.success) {
                this.analyzer.showMessage(`✅ ${data.message}`, 'success');

                if (data.popularity !== undefined) {
                    setTimeout(() => {
                        this.analyzer.showMessage(`📊 Keyword was used in ${data.popularity} game(s) before deletion.`, 'info', 3000);
                    }, 500);
                }

                if (keywordInput) {
                    keywordInput.value = '';
                }

                this.refreshCurrentKeywords();
                this.refreshFoundItems();

                if (data.analyze_after_delete) {
                    this.analyzer.showMessage('🔄 Performing text re-analysis...', 'info');
                    this.performAutoAnalysis(currentTab);
                }

            } else {
                this.analyzer.showMessage(`❌ ${data.message}`, 'error');
            }
        })
        .catch(error => {
            console.error('Error deleting keyword:', error);
            this.analyzer.showMessage('❌ Error deleting keyword: ' + error.message, 'error');
        })
        .finally(() => {
            if (deleteButton) {
                deleteButton.innerHTML = originalHTML;
                deleteButton.disabled = false;
            }
        });
    }

    performAutoAnalysis(tabName) {
        const form = document.getElementById('analyze-form');
        if (!form) {
            console.error('Analyze form not found');
            return;
        }

        const analyzeTabInput = document.getElementById('analyze-tab-input');
        if (analyzeTabInput) {
            analyzeTabInput.value = tabName;
        }

        const autoAnalyzeInput = document.getElementById('auto-analyze-input');
        if (autoAnalyzeInput) {
            autoAnalyzeInput.value = 'true';
        }

        let analyzeField = form.querySelector('input[name="analyze"]');
        if (!analyzeField) {
            analyzeField = document.createElement('input');
            analyzeField.type = 'hidden';
            analyzeField.name = 'analyze';
            analyzeField.value = 'true';
            form.appendChild(analyzeField);
        }

        this.analyzer.saveCurrentTab();
        this.analyzer.saveScrollPosition();
        this.analyzer.saveTabScrollPosition(tabName);

        setTimeout(() => {
            try {
                form.submit();
            } catch (error) {
                console.error('Error submitting form for auto-analysis:', error);
                this.analyzer.showMessage('❌ Error performing auto-analysis: ' + error.message, 'error');
            }
        }, 500);
    }

    refreshCurrentKeywords() {
        if (!this.gameId) {
            console.error('Cannot refresh keywords: Game ID not available');
            return;
        }

        const allCategories = document.querySelectorAll('.current-data-category');
        let currentKeywordsContainer = null;

        allCategories.forEach(container => {
            const h6 = container.querySelector('h6');
            if (h6 && h6.textContent.includes('Keywords')) {
                currentKeywordsContainer = container;
            }
        });

        if (!currentKeywordsContainer) {
            console.error('Keywords container not found');
            return;
        }

        fetch(`/games/${this.gameId}/analyze/current-keywords/`)
        .then(response => {
            const contentType = response.headers.get("content-type");
            if (!contentType || !contentType.includes("application/json")) {
                return response.text().then(text => {
                    throw new Error(`Expected JSON but got: ${text.substring(0, 100)}...`);
                });
            }
            return response.json();
        })
        .then(data => {
            if (data.success && data.keywords) {
                const itemsList = currentKeywordsContainer.querySelector('.current-items-list');
                if (itemsList) {
                    itemsList.innerHTML = data.keywords.map(keyword =>
                        `<span class="current-item">${keyword}</span>`
                    ).join('');
                }
            }
        })
        .catch(error => {
            console.error('Error refreshing keywords:', error);
            this.analyzer.showMessage('⚠️ Could not refresh keywords: ' + error.message, 'warning');
        });
    }

    refreshFoundItems() {
        if (!this.gameId) {
            console.error('Cannot refresh found items: Game ID not available');
            return;
        }

        const allCategories = document.querySelectorAll('.found-items-category');
        let foundKeywordsContainer = null;

        allCategories.forEach(container => {
            if (container.getAttribute('data-category') === 'keywords') {
                foundKeywordsContainer = container;
            }
        });

        if (!foundKeywordsContainer) {
            console.warn('Found keywords container not found');
            return;
        }

        const activeTab = this.analyzer.currentTab || 'summary';
        fetch(`/games/${this.gameId}/analyze/found-items/?tab=${activeTab}`)
        .then(response => {
            const contentType = response.headers.get("content-type");
            if (!contentType || !contentType.includes("application/json")) {
                return response.text().then(text => {
                    throw new Error(`Expected JSON but got: ${text.substring(0, 100)}...`);
                });
            }
            return response.json();
        })
        .then(data => {
            if (data.success && data.keywords) {
                const itemsList = foundKeywordsContainer.querySelector('.found-items-list');
                if (itemsList) {
                    itemsList.innerHTML = data.keywords.map(keyword =>
                        `<span class="badge ${keyword.is_new ? 'bg-warning text-dark' : 'bg-secondary'} found-item-badge"
                              data-name="${keyword.name}"
                              data-bs-toggle="tooltip"
                              title="${keyword.is_new ? 'New keyword (not saved yet) - Click to scroll to highlight' : 'Already exists in game - Click to scroll to highlight'}">
                            ${keyword.name}
                            ${keyword.is_new ? '<i class="bi bi-plus-circle ms-1"></i>' : '<i class="bi bi-check-circle ms-1"></i>'}
                        </span>`
                    ).join('');

                    const countBadge = foundKeywordsContainer.querySelector('h6 .badge');
                    if (countBadge) {
                        const newCount = data.keywords.filter(k => k.is_new).length;
                        countBadge.textContent = `${newCount} new`;
                    }
                }
            }
        })
        .catch(error => {
            console.error('Error refreshing found items:', error);
            this.analyzer.showMessage('⚠️ Could not refresh found items: ' + error.message, 'warning');
        });
    }

    getCSRFToken() {
        const csrfInput = document.querySelector('[name="csrfmiddlewaretoken"]');
        return csrfInput ? csrfInput.value : null;
    }
}

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
   ADD KEYWORD HANDLER
   ============================================ */

function bindAddKeywordButton(analyzer) {
    const addButton = document.getElementById('add-keyword-button');
    const keywordInput = document.getElementById('new-keyword-input');

    if (!addButton || !keywordInput) return;

    keywordInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();

            saveCurrentTab(analyzer);
            saveScrollPosition(analyzer);
            analyzer.saveTabScrollPosition(analyzer.currentTab);

            handleAddKeywordSubmission(analyzer);
        }
    });

    addButton.addEventListener('click', (e) => {
        e.preventDefault();

        saveCurrentTab(analyzer);
        saveScrollPosition(analyzer);
        analyzer.saveTabScrollPosition(analyzer.currentTab);

        handleAddKeywordSubmission(analyzer);
    });
}

function handleAddKeywordSubmission(analyzer) {
    const keywordInput = document.getElementById('new-keyword-input');
    const keyword = keywordInput ? keywordInput.value.trim() : '';

    if (!keyword) {
        showMessage('Please enter a keyword', 'error');
        return;
    }

    const csrfToken = getCSRFToken();
    if (!csrfToken) {
        showMessage('Security token missing', 'error');
        return;
    }

    const form = document.getElementById('analyze-form');
    if (!form) {
        showMessage('Form not found', 'error');
        return;
    }

    const autoAnalyzeInput = document.getElementById('auto-analyze-input');
    if (autoAnalyzeInput) {
        autoAnalyzeInput.value = 'true';
    }

    const newKeywordInput = document.createElement('input');
    newKeywordInput.type = 'hidden';
    newKeywordInput.name = 'new_keyword';
    newKeywordInput.value = keyword;
    form.appendChild(newKeywordInput);

    const addKeywordInput = document.createElement('input');
    addKeywordInput.type = 'hidden';
    addKeywordInput.name = 'add_keyword';
    addKeywordInput.value = 'true';
    form.appendChild(addKeywordInput);

    const currentTab = analyzer.currentTab;
    const analyzeTabInput = document.getElementById('analyze-tab-input');
    if (analyzeTabInput) {
        analyzeTabInput.value = currentTab;
    }

    setTimeout(() => {
        try {
            form.submit();
        } catch (error) {
            console.error('Error submitting form:', error);
            showMessage('Error adding keyword: ' + error.message, 'error');

            if (newKeywordInput.parentNode === form) {
                form.removeChild(newKeywordInput);
            }
            if (addKeywordInput.parentNode === form) {
                form.removeChild(addKeywordInput);
            }
        }
    }, 100);
}

function bindDeleteKeywordButton(analyzer) {
    console.log('Binding delete keyword button...');

    const deleteButton = document.getElementById('delete-keyword-button');
    console.log('Delete button found:', deleteButton);

    if (!deleteButton) {
        console.error('Delete button not found!');
        return;
    }

    const deleteHandler = new DeleteKeywordHandler(analyzer);
    deleteHandler.bind();

    if (!deleteHandler.gameId) {
        console.error('Failed to get game ID. Check HTML for <input id="game-id" value="...">');
    }
}

function bindNormalizeKeywordButton(analyzer) {
    console.log('Binding normalize keyword button...');
    const normalizeHandler = new NormalizeKeywordHandler(analyzer);
    normalizeHandler.bind();
}

/* ============================================
   TAB HANDLERS
   ============================================ */

function bindTabSelect(analyzer) {
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

function bindTabScrollEvents(analyzer) {
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

function bindBootstrapTabs(analyzer) {
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

function bindAnalyzeButton(analyzer) {
    if (!analyzer.elements.analyzeButton) {
        console.error('ANALYZE BUTTON NOT FOUND!');
        showMessage('Error: Analyze button not found. Please refresh page.', 'error');
        return;
    }

    const originalButton = analyzer.elements.analyzeButton;
    const newButton = originalButton.cloneNode(true);
    originalButton.parentNode.replaceChild(newButton, originalButton);
    analyzer.elements.analyzeButton = newButton;

    analyzer.elements.analyzeButton.addEventListener('click', (e) => {
        console.log('=== ANALYZE BUTTON CLICKED ===');
        e.preventDefault();
        e.stopPropagation();

        if (!analyzer.elements.analyzeForm) {
            console.error('Form element not found');
            showMessage('Error: Form not found', 'error');
            return;
        }

        saveCurrentTab(analyzer);
        saveScrollPosition(analyzer);
        analyzer.saveTabScrollPosition(analyzer.currentTab);
        analyzer.updateHiddenFields();

        const csrfToken = document.querySelector('[name="csrfmiddlewaretoken"]');
        if (!csrfToken) {
            console.error('CSRF token not found');
            showMessage('Error: Security token missing. Please refresh page.', 'error');
            return;
        }

        let analyzeField = analyzer.elements.analyzeForm.querySelector('input[name="analyze"]');
        if (!analyzeField) {
            analyzeField = document.createElement('input');
            analyzeField.type = 'hidden';
            analyzeField.name = 'analyze';
            analyzeField.value = 'true';
            analyzer.elements.analyzeForm.appendChild(analyzeField);
        }

        const originalHTML = analyzer.elements.analyzeButton.innerHTML;
        analyzer.elements.analyzeButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Analyzing...';
        analyzer.elements.analyzeButton.disabled = true;

        setTimeout(() => {
            try {
                analyzer.elements.analyzeForm.submit();
            } catch (error) {
                console.error('Error submitting form:', error);
                showMessage('Error submitting form: ' + error.message, 'error');

                analyzer.elements.analyzeButton.innerHTML = originalHTML;
                analyzer.elements.analyzeButton.disabled = false;
            }
        }, 300);
    });
}

function bindSaveButton(analyzer) {
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

function bindClearResultsButton(analyzer) {
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
            showMessage('Error: Clear URL not found', 'error');
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
                    showMessage('✅ ' + data.message, 'success');

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
            showMessage(`❌ Error clearing results: ${error.message}`, 'error');
            resetClearButton(clearBtn, originalHTML);
        }
    });
}

function bindBackToGameButton(analyzer) {
    if (!analyzer.elements.backToGameBtn) return;

    analyzer.elements.backToGameBtn.addEventListener('click', (e) => {
        clearTabStorage(analyzer);
        clearTabScrollPositions(analyzer);
        saveScrollPosition(analyzer);
    });
}

/* ============================================
   SCROLL HANDLERS
   ============================================ */

function bindScrollToTop(analyzer) {
    if (!analyzer.elements.scrollToTopBtn) return;

    analyzer.elements.scrollToTopBtn.addEventListener('click', () => analyzer.scrollToTop());
    window.addEventListener('scroll', () => analyzer.handleScroll());
}

/* ============================================
   FOUND ITEMS HANDLERS
   ============================================ */

function bindFoundItemsClicks(analyzer) {
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

/* ============================================
   UI SETUP HANDLERS
   ============================================ */

function setupTooltips(analyzer) {
    const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    tooltipTriggerList.forEach(tooltipTriggerEl => {
        const existingTooltip = window.bootstrap && bootstrap.Tooltip.getInstance(tooltipTriggerEl);
        if (existingTooltip) {
            existingTooltip.dispose();
        }
        if (window.bootstrap) {
            new bootstrap.Tooltip(tooltipTriggerEl, {
                trigger: 'hover focus',
                placement: 'top'
            });
        }
    });

    const activePane = document.querySelector(`#${analyzer.currentTab}.tab-pane.active`);
    if (activePane) {
        const highlights = activePane.querySelectorAll(`
            .highlight-genre, .highlight-theme, .highlight-perspective,
            .highlight-game_mode, .highlight-keyword, .highlight-multi
        `);

        highlights.forEach(highlight => {
            if (!highlight.hasAttribute('data-bs-toggle')) {
                if (highlight.classList.contains('highlight-multi')) {
                    highlight.removeAttribute('data-bs-toggle');
                    highlight.removeAttribute('data-bs-title');
                } else {
                    const elementName = highlight.dataset.elementName || 'Found element';
                    const category = highlight.dataset.category || 'element';
                    highlight.setAttribute('data-bs-toggle', 'tooltip');
                    highlight.setAttribute('data-bs-title', `${category}: ${elementName}`);
                    highlight.setAttribute('data-bs-placement', 'top');

                    if (window.bootstrap) {
                        const existingTooltip = bootstrap.Tooltip.getInstance(highlight);
                        if (existingTooltip) {
                            existingTooltip.dispose();
                        }
                        new bootstrap.Tooltip(highlight, {
                            trigger: 'hover focus',
                            placement: 'top'
                        });
                    }
                }
            }
        });
    }
}

function setupMultiCriteriaTooltips(analyzer) {
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

function setupHighlightEvents(analyzer) {
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
        showMessage('Error: Form not found. Cannot save results.', 'error');
        return;
    }

    const saveButton = analyzer.elements.analyzeForm.querySelector('button[name="save_results"]');
    if (saveButton) {
        const originalHTML = saveButton.innerHTML;
        saveButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Saving...';
        saveButton.disabled = true;
        saveButton.dataset.originalHTML = originalHTML;
    }

    showMessage('Saving results to database...', 'info');

    const saveInput = document.createElement('input');
    saveInput.type = 'hidden';
    saveInput.name = 'save_results';
    saveInput.value = 'true';
    analyzer.elements.analyzeForm.appendChild(saveInput);

    setTimeout(() => {
        try {
            analyzer.elements.analyzeForm.submit();
        } catch (error) {
            showMessage('❌ Error submitting form: ' + error.message, 'error');

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

function getCSRFToken() {
    const csrfInput = document.querySelector('[name="csrfmiddlewaretoken"]');
    return csrfInput ? csrfInput.value : null;
}

// Экспортируем все функции
export {
    bindAddKeywordButton,
    bindDeleteKeywordButton,
    bindNormalizeKeywordButton,
    bindTabSelect,
    bindTabScrollEvents,
    bindBootstrapTabs,
    bindAnalyzeButton,
    bindSaveButton,
    bindClearResultsButton,
    bindBackToGameButton,
    bindScrollToTop,
    bindFoundItemsClicks,
    setupTooltips,
    setupMultiCriteriaTooltips,
    setupHighlightEvents
};