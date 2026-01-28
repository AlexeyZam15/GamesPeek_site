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

        // ВОССТАНАВЛИВАЕМ СОХРАНЕННУЮ ВКЛАДКУ
        const restoredTab = restoreCurrentTab(this);

        // ВОССТАНАВЛИВАЕМ ПОЗИЦИЮ ПРОКРУТКИ ПОСЛЕ ЗАГРУЗКИ ВКЛАДКИ
        if (restoredTab) {
            this.restoreScrollPositionAfterTabLoad(restoredTab);
        } else if (this.currentTab) {
            this.restoreScrollPositionAfterTabLoad(this.currentTab);
        }

        console.log('Game Analyzer UI initialized');
    }

    /* ============================================
       SCROLL POSITION RESTORATION AFTER TAB LOAD
       ============================================ */

    restoreScrollPositionAfterTabLoad(tabName) {
        if (this.isRestoringScroll) return;

        this.isRestoringScroll = true;

        // Ждем, чтобы контент вкладки успел загрузиться
        setTimeout(() => {
            try {
                // Восстанавливаем позицию прокрутки для указанной вкладки
                restoreTabScrollPosition(this, tabName);

                // Также восстанавливаем глобальную позицию прокрутки окна
                restoreScrollPosition(this);
            } catch (error) {
                console.error('Error restoring scroll position:', error);
            } finally {
                this.isRestoringScroll = false;
            }
        }, 500);
    }

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