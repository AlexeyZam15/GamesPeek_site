// games/static/games/js/analyze/GameAnalyzerUI.js
/**
 * Основной класс Game Analyzer UI
 */

import {
    saveTabScrollPosition,
    restoreTabScrollPosition,
    saveScrollPosition,
    restoreScrollPosition,
    saveCurrentTab,
    restoreCurrentTab,
    clearTabStorage,
    clearTabScrollPositions
} from './utils.js';

import {
    bindTabSelect,
    bindTabScrollEvents,
    bindBootstrapTabs,
    bindAnalyzeButton,
    bindSaveButton,
    bindAddKeywordButton,
    bindDeleteKeywordButton,
    bindClearResultsButton,
    bindBackToGameButton,
    bindScrollToTop,
    bindFoundItemsClicks,
    setupTooltips,
    setupMultiCriteriaTooltips,
    setupHighlightEvents
} from './handlers.js';

import {
    switchTabByName,
    manualTabSwitch,
    updateUrlParam,
    removeUrlParam,
    forceTextAlignmentFix,
    scrollToHighlight,
    scrollToTop,
    handleScroll,
    showMessage
} from './managers.js';

class GameAnalyzerUI {
    constructor(options = {}) {
        this.options = {
            gameId: options.gameId || null,
            apiUrl: options.apiUrl || '',
            ...options
        };

        this.elements = {};
        this.currentTab = '';
        this.currentMode = 'combined';
        this.highlightedElements = new Set();
        this.currentMultiTooltip = null;
        this.isRestoringScroll = false;
        this.init();
    }

    /* ============================================
       INITIALIZATION METHODS
       ============================================ */

    init() {
        this.cacheElements();
        this.getCurrentState();
        this.bindEvents();
        setupTooltips(this);
        setupMultiCriteriaTooltips(this);
        setupHighlightEvents(this);
        this.handleScroll();
        this.loadUrlParams();
        this.forceTextAlignmentFix();
        this.checkForAutoAnalyze();

        const restoredTab = restoreCurrentTab(this);

        if (restoredTab) {
            this.restoreScrollPositionAfterTabLoad(restoredTab);
        } else if (this.currentTab) {
            this.restoreScrollPositionAfterTabLoad(this.currentTab);
        }

        console.log('Game Analyzer UI initialized');
    }

    // Обновление текущих ключевых слов игры
    refreshCurrentKeywords() {
        const currentKeywordsContainer = document.querySelector('.current-data-category:has(h6:contains("Keywords"))');
        if (!currentKeywordsContainer) return;

        // Обновляем через AJAX запрос
        fetch(`/games/${this.options.gameId}/analyze/current-keywords/`)
        .then(response => response.json())
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
        });
    }

    // Обновление найденных элементов
    refreshFoundItems() {
        const foundKeywordsContainer = document.querySelector('.found-items-category[data-category="keywords"]');
        if (!foundKeywordsContainer) return;

        // Обновляем через AJAX запрос
        const activeTab = this.currentTab;
        fetch(`/games/${this.options.gameId}/analyze/found-items/?tab=${activeTab}`)
        .then(response => response.json())
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

                    // Обновляем счетчик
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
        });
    }

    /* ============================================
       SCROLL POSITION RESTORATION AFTER TAB LOAD
       ============================================ */

    restoreScrollPositionAfterTabLoad(tabName) {
        if (this.isRestoringScroll) return;

        this.isRestoringScroll = true;

        setTimeout(() => {
            try {
                restoreTabScrollPosition(this, tabName);
                restoreScrollPosition(this);
            } catch (error) {
                console.error('Error restoring scroll position:', error);
            } finally {
                this.isRestoringScroll = false;
            }
        }, 500);
    }

    /* ============================================
       ADD KEYWORD METHOD - ДОБАВЛЕН НОВЫЙ МЕТОД
       ============================================ */

    handleAddKeyword() {
        const keywordInput = document.getElementById('new-keyword-input');
        const keyword = keywordInput ? keywordInput.value.trim() : '';

        if (!keyword) {
            this.showMessage('Please enter a keyword', 'error');
            return;
        }

        const csrfToken = this.getCSRFToken();
        if (!csrfToken) {
            this.showMessage('Security token missing', 'error');
            return;
        }

        const form = this.elements.analyzeForm;
        if (!form) {
            this.showMessage('Form not found', 'error');
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

        const currentTab = this.currentTab;
        const analyzeTabInput = document.getElementById('analyze-tab-input');
        if (analyzeTabInput) {
            analyzeTabInput.value = currentTab;
        }

        setTimeout(() => {
            try {
                form.submit();
            } catch (error) {
                console.error('Error submitting form:', error);
                this.showMessage('Error adding keyword: ' + error.message, 'error');

                if (newKeywordInput.parentNode === form) {
                    form.removeChild(newKeywordInput);
                }
                if (addKeywordInput.parentNode === form) {
                    form.removeChild(addKeywordInput);
                }
            }
        }, 100);
    }

    /* ============================================
       AUTO ANALYZE CHECK
       ============================================ */

    checkForAutoAnalyze() {
        const urlParams = new URLSearchParams(window.location.search);

        if (urlParams.get('cleared') === '1') {
            this.showMessage('✅ Несохранённые результаты успешно очищены.', 'success');
            setTimeout(() => this.removeUrlParam('cleared'), 3000);
        }

        if (urlParams.get('keyword_added') === '1') {
            this.showMessage('✅ Keyword added successfully!', 'success');

            if (urlParams.get('auto_analyze') === '1') {
                setTimeout(() => {
                    this.showMessage('🔍 Текст автоматически проанализирован после добавления ключевого слова. Все совпадения подсвечены.', 'info');
                }, 500);
            }

            setTimeout(() => {
                this.removeUrlParam('keyword_added');
                this.removeUrlParam('auto_analyze');
            }, 3000);
        }

        if (urlParams.get('saved') === '1') {
            this.showMessage('✅ Results saved successfully!', 'success');
            setTimeout(() => this.removeUrlParam('saved'), 3000);
        }
    }

    /* ============================================
       ELEMENT CACHING
       ============================================ */

    cacheElements() {
        this.elements = {
            modeButtons: document.querySelectorAll('.mode-btn'),
            analyzeForm: document.getElementById('analyze-form'),
            tabSelect: document.getElementById('analyze-tab-select'),
            newKeywordInput: document.getElementById('new-keyword-input'),
            addKeywordButton: document.getElementById('add-keyword-button'),
            highlightToggle: document.getElementById('highlight-toggle'),
            analyzeTabs: document.getElementById('analyzeTabs'),
            analyzeTabLinks: document.querySelectorAll('#analyzeTabs .nav-link'),
            analyzeTabPanes: document.querySelectorAll('.tab-pane'),
            scrollToTopBtn: document.querySelector('.scroll-to-top'),
            tabInput: document.getElementById('analyze-tab-input'),
            modeInput: document.getElementById('analyze-mode-input'),
            analyzeButton: document.getElementById('analyze-button'),
            saveButton: document.getElementById('save-button'),
            clearResultsBtn: document.getElementById('clear-results-btn'),
            backToGameBtn: document.getElementById('back-to-game-btn'),
            gameIdInput: document.getElementById('game-id')
        };

        console.log('Elements cached:', Object.keys(this.elements).filter(k => this.elements[k]));
    }

    getCurrentState() {
        const activeTabLink = document.querySelector('#analyzeTabs .nav-link.active');
        if (activeTabLink) {
            const href = activeTabLink.getAttribute('href');
            this.currentTab = href ? href.substring(1) : '';
        } else if (this.elements.analyzeTabLinks && this.elements.analyzeTabLinks.length > 0) {
            const firstTab = this.elements.analyzeTabLinks[0];
            const href = firstTab.getAttribute('href');
            this.currentTab = href ? href.substring(1) : '';
        }

        this.currentMode = 'combined';
    }

    /* ============================================
       EVENT BINDING
       ============================================ */

    bindEvents() {
        console.log('=== BINDING EVENTS ===');

        bindTabSelect(this);
        bindTabScrollEvents(this);
        bindAnalyzeButton(this);
        bindSaveButton(this);
        bindAddKeywordButton(this);
        bindDeleteKeywordButton(this);  // ← УБЕДИТЬСЯ ЧТО ЭТО ЕСТЬ
        bindClearResultsButton(this);
        bindBackToGameButton(this);
        bindBootstrapTabs(this);
        bindScrollToTop(this);
        bindFoundItemsClicks(this);

        console.log('=== ALL EVENTS BOUND ===');
    }

    /* ============================================
       TAB & SCROLL MANAGEMENT
       ============================================ */

    loadUrlParams() {
        const urlParams = new URLSearchParams(window.location.search);
        const tab = urlParams.get('tab');

        if (tab) {
            this.switchTabByName(tab);
        }
    }

    switchTabByName(tabName) {
        return switchTabByName(this, tabName);
    }

    manualTabSwitch(tabName) {
        return manualTabSwitch(this, tabName);
    }

    onTabSwitch(tabName) {
        if (this.currentTab === tabName) return;

        if (this.elements.tabSelect) {
            this.elements.tabSelect.value = tabName;
        }

        if (this.elements.tabInput) {
            this.elements.tabInput.value = tabName;
        }

        this.currentTab = tabName;

        setTimeout(() => {
            this.forceTextAlignmentFix();
            setupTooltips(this);
        }, 10);

        this.updateUrlParam('tab', tabName);
    }

    updateUrlParam(key, value) {
        return updateUrlParam(key, value);
    }

    removeUrlParam(param) {
        return removeUrlParam(param);
    }

    /* ============================================
       TEXT ALIGNMENT
       ============================================ */

    forceTextAlignmentFix() {
        return forceTextAlignmentFix(this);
    }

    /* ============================================
       ACTIONS
       ============================================ */

    scrollToHighlight(elementName) {
        return scrollToHighlight(this, elementName);
    }

    scrollToTop() {
        return scrollToTop();
    }

    handleScroll() {
        return handleScroll(this);
    }

    /* ============================================
       UTILITY METHODS
       ============================================ */

    showMessage(text, type = 'info', duration = 5000) {
        return showMessage(text, type, duration);
    }

    getCSRFToken() {
        const csrfInput = document.querySelector('[name="csrfmiddlewaretoken"]');
        return csrfInput ? csrfInput.value : null;
    }

    updateHiddenFields() {
        if (this.elements.tabSelect && this.elements.tabInput) {
            const selectedTab = this.elements.tabSelect.value;
            this.elements.tabInput.value = selectedTab;
        }

        if (this.elements.modeInput) {
            this.elements.modeInput.value = 'combined';
        }
    }

    /* ============================================
       TAB PERSISTENCE METHODS (реэкспортированные)
       ============================================ */

    saveCurrentTab() {
        return saveCurrentTab(this);
    }

    restoreCurrentTab() {
        return restoreCurrentTab(this);
    }

    clearTabStorage() {
        return clearTabStorage(this);
    }

    /* ============================================
       SCROLL POSITION METHODS (реэкспортированные)
       ============================================ */

    saveScrollPosition() {
        return saveScrollPosition(this);
    }

    restoreScrollPosition() {
        return restoreScrollPosition(this);
    }

    saveTabScrollPosition(tabName) {
        return saveTabScrollPosition(this, tabName);
    }

    restoreTabScrollPosition(tabName) {
        return restoreTabScrollPosition(this, tabName);
    }

    clearTabScrollPositions() {
        return clearTabScrollPositions(this);
    }
}

export default GameAnalyzerUI;