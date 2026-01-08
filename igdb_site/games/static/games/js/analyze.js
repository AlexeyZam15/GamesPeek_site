// games/static/games/js/analyze.js

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
        this.init();
    }

    /* ============================================
       INITIALIZATION METHODS
       ============================================ */

    init() {
        this.cacheElements();
        this.getCurrentState();
        this.bindEvents();
        this.setupTooltips();
        this.handleScroll();
        this.loadUrlParams();
        this.forceTextAlignmentFix();
        this.restoreScrollPosition();
        this.setupHighlightEvents();
        this.checkForAutoAnalyze();

        // ВОССТАНАВЛИВАЕМ СОХРАНЕННУЮ ВКЛАДКУ
        this.restoreCurrentTab();

        console.log('Game Analyzer UI initialized');
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

    setupHighlightEvents() {
        // Обработка наведения на подсвеченные элементы (только hover)
        document.addEventListener('mouseenter', (e) => {
            const highlightElement = e.target.closest('.highlight-genre, .highlight-theme, .highlight-perspective, .highlight-game_mode, .highlight-keyword');

            if (highlightElement) {
                this.handleHighlightHover(highlightElement, true);
            }
        }, true);

        document.addEventListener('mouseleave', (e) => {
            const highlightElement = e.target.closest('.highlight-genre, .highlight-theme, .highlight-perspective, .highlight-game_mode, .highlight-keyword');

            if (highlightElement) {
                this.handleHighlightHover(highlightElement, false);
            }
        }, true);
    }

    handleHighlightHover(element, isEntering) {
        if (isEntering) {
            element.classList.add('highlight-hover');
        } else {
            element.classList.remove('highlight-hover');
        }
    }

    flashElement(element) {
        element.classList.add('highlight-flash');
        setTimeout(() => {
            element.classList.remove('highlight-flash');
        }, 2000);
    }

    showElementInfo(name, category) {
        const categoryNames = {
            'genres': 'Genre',
            'themes': 'Theme',
            'perspectives': 'Perspective',
            'game_modes': 'Game Mode',
            'keywords': 'Keyword'
        };

        const categoryName = categoryNames[category] || 'Element';
        this.showMessage(`${categoryName}: <strong>${name}</strong>`, 'info', 2000);
    }

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
       TAB PERSISTENCE METHODS
       ============================================ */

    saveCurrentTab() {
        // Сохраняем текущую вкладку в sessionStorage
        if (this.currentTab) {
            sessionStorage.setItem(`analyze_active_tab_${this.options.gameId}`, this.currentTab);

            // Также сохраняем в localStorage для долгосрочного хранения
            localStorage.setItem(`analyze_active_tab_${this.options.gameId}`, this.currentTab);

            console.log(`Tab saved: ${this.currentTab} for game ${this.options.gameId}`);
        }
    }

    restoreCurrentTab() {
        try {
            // Проверяем, есть ли вкладка в URL параметрах
            const urlParams = new URLSearchParams(window.location.search);
            const tabFromUrl = urlParams.get('tab');

            // Если есть вкладка в URL, используем её (самый высокий приоритет)
            if (tabFromUrl) {
                const tabExists = document.querySelector(`#analyzeTabs a[href="#${tabFromUrl}"]`);
                if (tabExists) {
                    setTimeout(() => {
                        this.switchTabByName(tabFromUrl);
                    }, 100);
                    return tabFromUrl;
                }
            }

            // Если нет в URL, пробуем получить из sessionStorage
            let savedTab = sessionStorage.getItem(`analyze_active_tab_${this.options.gameId}`);

            // Если нет, пробуем из localStorage
            if (!savedTab) {
                savedTab = localStorage.getItem(`analyze_active_tab_${this.options.gameId}`);
            }

            // Если есть сохраненная вкладка и она существует на странице
            if (savedTab) {
                const tabExists = document.querySelector(`#analyzeTabs a[href="#${savedTab}"]`);
                if (tabExists) {
                    // Ждем немного, чтобы DOM полностью загрузился
                    setTimeout(() => {
                        this.switchTabByName(savedTab);
                    }, 150);
                    return savedTab;
                }
            }

            // Если ничего не нашли, но есть активная вкладка по умолчанию
            if (this.currentTab) {
                this.saveCurrentTab();
            }
        } catch (error) {
            console.error('Error restoring tab:', error);
        }

        return null;
    }

    clearTabStorage() {
        // Очищаем сохраненные данные о вкладках
        sessionStorage.removeItem(`analyze_active_tab_${this.options.gameId}`);
        localStorage.removeItem(`analyze_active_tab_${this.options.gameId}`);
    }

    /* ============================================
       SCROLL POSITION MANAGEMENT
       ============================================ */

    saveScrollPosition() {
        // Сохраняем позицию прокрутки
        sessionStorage.setItem(`analyze_scroll_y_${this.options.gameId}`, window.scrollY.toString());
    }

    restoreScrollPosition() {
        try {
            const savedScrollY = sessionStorage.getItem(`analyze_scroll_y_${this.options.gameId}`);
            if (savedScrollY) {
                const scrollY = parseInt(savedScrollY);
                if (scrollY > 0) {
                    // Прокручиваем после небольшой задержки
                    setTimeout(() => {
                        window.scrollTo(0, scrollY);
                        // Очищаем сохраненную позицию
                        sessionStorage.removeItem(`analyze_scroll_y_${this.options.gameId}`);
                    }, 100);
                }
            }
        } catch (error) {
            console.error('Error restoring scroll position:', error);
            sessionStorage.removeItem(`analyze_scroll_y_${this.options.gameId}`);
        }
    }

    /* ============================================
       EVENT HANDLING METHODS
       ============================================ */

    bindEvents() {
        console.log('=== BINDING EVENTS ===');

        this.bindTabSelect();
        this.bindHighlightToggle();
        this.bindAnalyzeButton();
        this.bindSaveButton();
        this.bindAddKeywordButton();
        this.bindClearResultsButton();
        this.bindBackToGameButton();
        this.bindBootstrapTabs();
        this.bindScrollToTop();
        this.bindFoundItemsClicks();

        console.log('=== ALL EVENTS BOUND ===');
    }

    bindTabSelect() {
        if (!this.elements.tabSelect) return;

        this.elements.tabSelect.addEventListener('change', (e) => {
            const tabName = e.target.value;
            console.log(`Tab select changed to: ${tabName}`);

            if (this.elements.tabInput) {
                this.elements.tabInput.value = tabName;
            }

            this.switchTabByName(tabName);

            // СОХРАНЯЕМ ВЫБРАННУЮ ВКЛАДКУ
            this.saveCurrentTab();
        });
    }

    bindHighlightToggle() {
        if (!this.elements.highlightToggle) return;

        this.elements.highlightToggle.addEventListener('change', (e) => {
            console.log(`Highlight toggle: ${e.target.checked}`);
            this.handleHighlightToggle(e);
        });
    }

    bindAnalyzeButton() {
        if (!this.elements.analyzeButton) {
            console.error('ANALYZE BUTTON NOT FOUND!');
            this.showMessage('Error: Analyze button not found. Please refresh page.', 'error');
            return;
        }

        const originalButton = this.elements.analyzeButton;
        const newButton = originalButton.cloneNode(true);
        originalButton.parentNode.replaceChild(newButton, originalButton);
        this.elements.analyzeButton = newButton;

        this.elements.analyzeButton.addEventListener('click', (e) => {
            console.log('=== ANALYZE BUTTON CLICKED ===');
            e.preventDefault();
            e.stopPropagation();

            if (!this.elements.analyzeForm) {
                console.error('Form element not found');
                this.showMessage('Error: Form not found', 'error');
                return;
            }

            // СОХРАНЯЕМ ТЕКУЩУЮ ВКЛАДКУ ПЕРЕД ОТПРАВКОЙ
            this.saveCurrentTab();
            this.saveScrollPosition();
            this.updateHiddenFields();

            const csrfToken = document.querySelector('[name="csrfmiddlewaretoken"]');
            if (!csrfToken) {
                console.error('CSRF token not found');
                this.showMessage('Error: Security token missing. Please refresh page.', 'error');
                return;
            }

            // Убедимся, что есть поле analyze
            let analyzeField = this.elements.analyzeForm.querySelector('input[name="analyze"]');
            if (!analyzeField) {
                analyzeField = document.createElement('input');
                analyzeField.type = 'hidden';
                analyzeField.name = 'analyze';
                analyzeField.value = 'true';
                this.elements.analyzeForm.appendChild(analyzeField);
            }

            const originalHTML = this.elements.analyzeButton.innerHTML;
            this.elements.analyzeButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Analyzing...';
            this.elements.analyzeButton.disabled = true;

            setTimeout(() => {
                try {
                    this.elements.analyzeForm.submit();
                } catch (error) {
                    console.error('Error submitting form:', error);
                    this.showMessage('Error submitting form: ' + error.message, 'error');

                    this.elements.analyzeButton.innerHTML = originalHTML;
                    this.elements.analyzeButton.disabled = false;
                }
            }, 300);
        });
    }

    bindSaveButton() {
        if (!this.elements.saveButton) return;

        const newButton = this.elements.saveButton.cloneNode(true);
        this.elements.saveButton.parentNode.replaceChild(newButton, this.elements.saveButton);
        this.elements.saveButton = newButton;

        this.elements.saveButton.addEventListener('click', (e) => {
            e.preventDefault();

            // СОХРАНЯЕМ ТЕКУЩУЮ ВКЛАДКУ ПЕРЕД ОТПРАВКОЙ
            this.saveCurrentTab();
            this.saveScrollPosition();
            this.handleSaveResults(e);
        });
    }

    bindAddKeywordButton() {
        const addButton = document.getElementById('add-keyword-button');
        const keywordInput = document.getElementById('new-keyword-input');

        if (!addButton || !keywordInput) return;

        keywordInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();

                // СОХРАНЯЕМ ТЕКУЩУЮ ВКЛАДКУ ПЕРЕД ОТПРАВКОЙ
                this.saveCurrentTab();
                this.saveScrollPosition();
                this.handleAddKeyword();
            }
        });

        addButton.addEventListener('click', (e) => {
            e.preventDefault();

            // СОХРАНЯЕМ ТЕКУЩУЮ ВКЛАДКУ ПЕРЕД ОТПРАВКОЙ
            this.saveCurrentTab();
            this.saveScrollPosition();
            this.handleAddKeyword();
        });
    }

    bindClearResultsButton() {
        if (!this.elements.clearResultsBtn) return;

        const originalButton = this.elements.clearResultsBtn;
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

            // СОХРАНЯЕМ ТЕКУЩУЮ ВКЛАДКУ ПЕРЕД ОТПРАВКОЙ
            this.saveCurrentTab();
            this.saveScrollPosition();

            const originalHTML = clearBtn.innerHTML;
            clearBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Clearing...';
            clearBtn.disabled = true;

            const clearUrl = clearBtn.getAttribute('href');

            if (!clearUrl) {
                console.error('Clear URL not found');
                this.showMessage('Error: Clear URL not found', 'error');
                this.resetClearButton(clearBtn, originalHTML);
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
                        this.showMessage('✅ ' + data.message, 'success');

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
                this.showMessage(`❌ Error clearing results: ${error.message}`, 'error');
                this.resetClearButton(clearBtn, originalHTML);
            }
        });
    }

    bindBackToGameButton() {
        if (!this.elements.backToGameBtn) return;

        this.elements.backToGameBtn.addEventListener('click', (e) => {
            // Очищаем сохраненные данные о вкладках при переходе назад
            this.clearTabStorage();
            this.saveScrollPosition();
        });
    }

    bindBootstrapTabs() {
        if (!this.elements.analyzeTabLinks || this.elements.analyzeTabLinks.length === 0) return;

        this.elements.analyzeTabLinks.forEach(link => {
            link.addEventListener('shown.bs.tab', (e) => {
                const href = e.target.getAttribute('href');
                if (href) {
                    const tabName = href.substring(1);
                    this.onTabSwitch(tabName);

                    // СОХРАНЯЕМ ВЫБРАННУЮ ВКЛАДКУ
                    this.saveCurrentTab();
                }
            });
        });
    }

    bindScrollToTop() {
        if (!this.elements.scrollToTopBtn) return;

        this.elements.scrollToTopBtn.addEventListener('click', () => this.scrollToTop());
        window.addEventListener('scroll', () => this.handleScroll());
    }

    bindFoundItemsClicks() {
        document.addEventListener('click', (e) => {
            const foundItem = e.target.closest('.found-item-badge');
            if (foundItem) {
                e.preventDefault();
                const elementName = foundItem.dataset.name;
                if (elementName) {
                    // Убираем выделение с предыдущих элементов в списке
                    const previousSelected = document.querySelectorAll('.found-item-badge.selected');
                    previousSelected.forEach(el => {
                        el.classList.remove('selected');
                    });

                    // Выделяем текущий элемент в списке
                    foundItem.classList.add('selected');

                    // Прокручиваем к подсвеченным словам
                    this.scrollToHighlight(elementName);
                }
            }
        });
    }

    resetClearButton(button, originalHTML) {
        if (button) {
            button.innerHTML = originalHTML;
            button.disabled = false;
        }
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

    handleHighlightToggle(e) {
        const isEnabled = e.target.checked;

        if (this.elements.analyzeForm) {
            let toggleInput = this.elements.analyzeForm.querySelector('input[name="highlight_toggle_only"]');
            if (!toggleInput) {
                toggleInput = document.createElement('input');
                toggleInput.type = 'hidden';
                toggleInput.name = 'highlight_toggle_only';
                toggleInput.value = '1';
                this.elements.analyzeForm.appendChild(toggleInput);
            }
            toggleInput.value = '1';

            let highlightInput = this.elements.analyzeForm.querySelector('input[name="highlight_toggle"]');
            if (!highlightInput) {
                highlightInput = document.createElement('input');
                highlightInput.type = 'hidden';
                highlightInput.name = 'highlight_toggle';
                this.elements.analyzeForm.appendChild(highlightInput);
            }
            highlightInput.value = isEnabled ? 'on' : 'off';

            // СОХРАНЯЕМ ТЕКУЩУЮ ВКЛАДКУ ПЕРЕД ОТПРАВКОЙ
            this.saveCurrentTab();
            this.saveScrollPosition();

            this.showMessage('Highlight setting updated. Click "Analyze Text" to apply changes.', 'info');
        } else {
            this.showMessage('Error: Cannot toggle highlighting. Form not found.', 'error');
        }
    }

    handleAddKeyword() {
        const keywordInput = document.getElementById('new-keyword-input');
        const addButton = document.getElementById('add-keyword-button');

        if (!keywordInput || !addButton) {
            console.error('Add keyword elements not found');
            return;
        }

        const keyword = keywordInput.value.trim();
        const gameId = this.elements.gameIdInput ? this.elements.gameIdInput.value : null;

        if (!keyword) {
            this.showMessage('Please enter a keyword', 'warning');
            keywordInput.focus();
            return;
        }

        if (!gameId) {
            this.showMessage('Error: Game ID not found', 'error');
            return;
        }

        const originalButtonText = addButton.innerHTML;
        const originalButtonState = addButton.disabled;

        addButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Adding and Analyzing...';
        addButton.disabled = true;

        // ПОЛУЧАЕМ ТЕКУЩУЮ АКТИВНУЮ ВКЛАДКУ
        const currentTab = this.currentTab || 'summary';
        console.log(`Adding keyword to current tab: ${currentTab}`);

        const formData = new FormData();
        formData.append('add_keyword', 'true');
        formData.append('new_keyword', keyword);
        formData.append('analyze_tab', currentTab); // ИСПРАВЛЕНИЕ: используем currentTab вместо this.currentTab
        formData.append('auto_analyze', 'true');

        const csrfToken = this.getCSRFToken();
        if (csrfToken) {
            formData.append('csrfmiddlewaretoken', csrfToken);
        }

        const form = this.elements.analyzeForm;
        const originalAction = form ? form.action : window.location.href;

        fetch(originalAction, {
            method: 'POST',
            body: formData,
            headers: {
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => {
            if (response.redirected) {
                window.location.href = response.url;
                return null;
            }
            return response.json();
        })
        .catch(error => {
            console.error('Error adding keyword:', error);
            this.showMessage(`❌ Error adding keyword: ${error.message}`, 'error');
            addButton.innerHTML = originalButtonText;
            addButton.disabled = originalButtonState;
        });
    }

    getCSRFToken() {
        const csrfInput = document.querySelector('[name="csrfmiddlewaretoken"]');
        return csrfInput ? csrfInput.value : null;
    }

    updateCurrentKeywordsList(keywordId, keywordName) {
        const currentKeywordsContainer = document.querySelector('#current-items-container .current-items-list');
        if (!currentKeywordsContainer) return;

        const newKeywordElement = document.createElement('span');
        newKeywordElement.className = 'current-item';
        newKeywordElement.textContent = keywordName;
        currentKeywordsContainer.prepend(newKeywordElement);
    }

    refreshFoundElementsList(category, item) {
        const foundItemsContainer = document.getElementById('found-items-container');
        if (!foundItemsContainer) return;

        let categoryElement = foundItemsContainer.querySelector(`.found-items-category[data-category="${category}"]`);

        if (!categoryElement) {
            categoryElement = document.createElement('div');
            categoryElement.className = 'found-items-category';
            categoryElement.dataset.category = category;

            const categoryTitle = document.createElement('h6');
            categoryTitle.innerHTML = `${this.capitalizeFirstLetter(category)} (1)`;
            categoryElement.appendChild(categoryTitle);

            const itemsList = document.createElement('div');
            itemsList.className = 'found-items-list';
            categoryElement.appendChild(itemsList);

            foundItemsContainer.prepend(categoryElement);
        }

        const categoryTitle = categoryElement.querySelector('h6');
        const currentCount = categoryElement.querySelectorAll('.found-item-badge').length + 1;
        categoryTitle.innerHTML = `${this.capitalizeFirstLetter(category)} (${currentCount})`;

        const itemsList = categoryElement.querySelector('.found-items-list');
        const newBadge = document.createElement('span');

        let badgeClass = 'bg-secondary';
        if (category === 'keywords') badgeClass = 'bg-warning text-dark';
        else if (category === 'genres') badgeClass = 'bg-success';
        else if (category === 'themes') badgeClass = 'bg-danger';
        else if (category === 'perspectives') badgeClass = 'bg-primary';
        else if (category === 'game_modes') badgeClass = 'bg-purple';

        newBadge.className = `badge ${badgeClass} found-item-badge`;
        newBadge.dataset.name = item.name;
        newBadge.dataset.category = category;
        newBadge.setAttribute('data-bs-toggle', 'tooltip');
        newBadge.setAttribute('title', `New ${category} (added manually) - Click to scroll to highlight`);
        newBadge.innerHTML = `${item.name} <i class="bi bi-plus-circle ms-1"></i>`;

        itemsList.appendChild(newBadge);

        if (window.bootstrap) {
            new bootstrap.Tooltip(newBadge);
        }
    }

    capitalizeFirstLetter(string) {
        return string.charAt(0).toUpperCase() + string.slice(1);
    }

    /* ============================================
       TAB & MODE MANAGEMENT
       ============================================ */

    loadUrlParams() {
        const urlParams = new URLSearchParams(window.location.search);
        const tab = urlParams.get('tab');

        if (tab) {
            this.switchTabByName(tab);
        }
    }

    switchTabByName(tabName) {
        const tabLink = document.querySelector(`#analyzeTabs a[href="#${tabName}"]`);
        if (tabLink) {
            try {
                if (window.bootstrap) {
                    const tab = new bootstrap.Tab(tabLink);
                    tab.show();
                } else {
                    this.manualTabSwitch(tabName);
                }
            } catch (error) {
                this.manualTabSwitch(tabName);
            }
        }
    }

    manualTabSwitch(tabName) {
        const tabLinks = document.querySelectorAll('#analyzeTabs .nav-link');
        const tabPanes = document.querySelectorAll('.tab-pane');

        tabLinks.forEach(link => {
            link.classList.remove('active');
            link.setAttribute('aria-selected', 'false');
        });

        tabPanes.forEach(pane => {
            pane.classList.remove('show', 'active');
        });

        const targetLink = document.querySelector(`#analyzeTabs a[href="#${tabName}"]`);
        const targetPane = document.getElementById(tabName);

        if (targetLink && targetPane) {
            targetLink.classList.add('active');
            targetLink.setAttribute('aria-selected', 'true');
            targetPane.classList.add('show', 'active');
            this.onTabSwitch(tabName);
        }
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
            this.setupTooltips();
        }, 10);

        this.updateUrlParam('tab', tabName);
    }

    updateUrlParam(key, value) {
        try {
            const url = new URL(window.location);
            url.searchParams.set(key, value);
            window.history.replaceState({}, '', url);
        } catch (e) {
            console.error('Failed to update URL:', e);
        }
    }

    removeUrlParam(param) {
        try {
            const url = new URL(window.location);
            const params = new URLSearchParams(url.search);
            params.delete(param);
            const newUrl = `${url.pathname}${params.toString() ? '?' + params.toString() : ''}${url.hash}`;
            window.history.replaceState({}, '', newUrl);
            console.log(`URL parameter "${param}" removed`);
        } catch (e) {
            console.error('Failed to remove URL param:', e);
        }
    }

    /* ============================================
       TEXT ALIGNMENT FIX METHODS
       ============================================ */

    forceTextAlignmentFix() {
        const activePane = document.querySelector(`#${this.currentTab}.tab-pane.active`);
        if (!activePane) return;

        const textContent = activePane.querySelector('.text-content');
        if (!textContent) return;

        textContent.style.cssText = `
            text-align: left !important;
            margin: 0 !important;
            padding: 0 !important;
            position: static !important;
            transform: none !important;
            display: block !important;
            vertical-align: top !important;
        `;

        const paragraphs = textContent.querySelectorAll('p');
        paragraphs.forEach(p => {
            p.style.cssText = `
                text-align: left !important;
                margin-bottom: 1.25em !important;
                margin-top: 0 !important;
            `;
        });

        const headings = textContent.querySelectorAll('h1, h2, h3, h4, h5, h6');
        headings.forEach(h => {
            h.style.cssText = 'text-align: left !important;';
        });

        const divs = textContent.querySelectorAll('div');
        divs.forEach(div => {
            if (div.classList.contains('text-center') ||
                div.classList.contains('align-center') ||
                div.classList.contains('mx-auto') ||
                div.style.textAlign === 'center') {
                div.style.cssText = `
                    text-align: left !important;
                    margin: 0 !important;
                    display: block !important;
                `;
            }
        });
    }

    /* ============================================
       ACTION METHODS
       ============================================ */

    createMatchCountIndicator(element, count) {
        // Удаляем старый индикатор, если есть
        const oldIndicator = element.querySelector('.highlight-match-count');
        if (oldIndicator) oldIndicator.remove();

        if (count > 1) {
            const indicator = document.createElement('span');
            indicator.className = 'highlight-match-count';
            indicator.textContent = count;
            element.style.position = 'relative';
            element.appendChild(indicator);

            // Автоматически скрываем через 3 секунды
            setTimeout(() => {
                if (indicator.parentNode === element) {
                    indicator.remove();
                }
            }, 3000);
        }
    }

    scrollToHighlight(elementName) {
        const activePane = document.querySelector(`#${this.currentTab}.tab-pane.active`);
        if (!activePane) return;

        const highlights = activePane.querySelectorAll(`[data-element-name="${elementName}"]`);
        if (highlights.length > 0) {
            let targetHighlight = highlights[0];

            // Сначала убираем предыдущее специальное выделение
            const previousSpecial = activePane.querySelectorAll('.highlight-found-element');
            previousSpecial.forEach(el => {
                el.classList.remove('highlight-found-element', 'highlight-blink', 'highlight-gradient');
            });

            // Прокручиваем к первому элементу
            targetHighlight.scrollIntoView({
                behavior: 'smooth',
                block: 'center'
            });

            // Применяем специальное выделение ко ВСЕМ совпадениям
            highlights.forEach((h, index) => {
                // Добавляем классы специальной подсветки
                h.classList.add('highlight-found-element', 'highlight-blink', 'highlight-transition');

                // Добавляем градиентный эффект
                h.classList.add('highlight-gradient');

                // Добавляем анимацию с задержкой для создания волны
                setTimeout(() => {
                    h.style.animation = 'highlightFoundElementPulse 2s ease-in-out';
                }, index * 50);

                // Добавляем flash анимацию
                this.flashElement(h);

                // Добавляем индикатор количества совпадений
                this.createMatchCountIndicator(h, highlights.length);
            });

            // Показываем сообщение о количестве найденных совпадений
            if (highlights.length > 1) {
                this.showMessage(`Found ${highlights.length} occurrences of "${elementName}"`, 'info', 3000);
            }
        }
    }

    scrollToTop() {
        window.scrollTo({
            top: 0,
            behavior: 'smooth'
        });
    }

    handleSaveResults(e) {
        e.preventDefault();

        if (!confirm('Are you sure you want to save the analysis results to the game database?\n\nThis will add new elements (genres, themes, keywords, etc.) to the game.')) {
            console.log('Save cancelled by user');
            return;
        }

        if (!this.elements.analyzeForm) {
            this.showMessage('Error: Form not found. Cannot save results.', 'error');
            return;
        }

        const saveButton = this.elements.analyzeForm.querySelector('button[name="save_results"]');
        if (saveButton) {
            const originalHTML = saveButton.innerHTML;
            saveButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Saving...';
            saveButton.disabled = true;
            saveButton.dataset.originalHTML = originalHTML;
        }

        this.showMessage('Saving results to database...', 'info');

        const saveInput = document.createElement('input');
        saveInput.type = 'hidden';
        saveInput.name = 'save_results';
        saveInput.value = 'true';
        this.elements.analyzeForm.appendChild(saveInput);

        setTimeout(() => {
            try {
                this.elements.analyzeForm.submit();
            } catch (error) {
                this.showMessage('❌ Error submitting form: ' + error.message, 'error');

                if (saveButton && saveButton.dataset.originalHTML) {
                    saveButton.innerHTML = saveButton.dataset.originalHTML;
                    saveButton.disabled = false;
                }
            }
        }, 100);
    }

    /* ============================================
       UTILITY METHODS
       ============================================ */

    setupTooltips() {
        // Тулкиты для Bootstrap элементов
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

        // Тулкиты для подсвеченных элементов
        const activePane = document.querySelector(`#${this.currentTab}.tab-pane.active`);
        if (activePane) {
            const highlights = activePane.querySelectorAll('.highlight-genre, .highlight-theme, .highlight-perspective, .highlight-game_mode, .highlight-keyword');
            highlights.forEach(highlight => {
                if (!highlight.hasAttribute('data-bs-toggle')) {
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
            });
        }
    }

    handleScroll() {
        if (!this.elements.scrollToTopBtn) return;

        if (window.scrollY > 300) {
            this.elements.scrollToTopBtn.classList.add('visible');
        } else {
            this.elements.scrollToTopBtn.classList.remove('visible');
        }
    }

    showMessage(text, type = 'info', duration = 5000) {
        // Удаляем старые сообщения
        const oldAlerts = document.querySelectorAll('.analyzer-alert');
        oldAlerts.forEach(alert => {
            if (window.bootstrap) {
                const bsAlert = bootstrap.Alert.getInstance(alert);
                if (bsAlert) {
                    bsAlert.close();
                } else {
                    alert.remove();
                }
            } else {
                alert.remove();
            }
        });

        // Создаем новое сообщение
        const alert = document.createElement('div');
        alert.className = `alert alert-${type} alert-dismissible fade show analyzer-alert position-fixed`;
        alert.style.cssText = `
            top: 20px;
            right: 20px;
            z-index: 1060;
            min-width: 300px;
            max-width: 500px;
            box-shadow: 0 8px 32px rgba(0,0,0,0.3);
            animation: slideInRight 0.3s ease;
        `;

        const icons = {
            'success': 'bi-check-circle',
            'error': 'bi-x-circle',
            'warning': 'bi-exclamation-triangle',
            'info': 'bi-info-circle'
        };

        alert.innerHTML = `
            <div class="d-flex align-items-center">
                <i class="bi ${icons[type] || 'bi-info-circle'} me-3 fs-5 text-${type}"></i>
                <div class="flex-grow-1">${text}</div>
                <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
            </div>
        `;

        document.body.appendChild(alert);

        // Инициализируем Bootstrap Alert
        if (window.bootstrap) {
            new bootstrap.Alert(alert);
        }

        // Автоматически скрываем через duration (кроме ошибок)
        if (type !== 'error' && duration > 0) {
            setTimeout(() => {
                if (alert.parentNode) {
                    if (window.bootstrap) {
                        const bsAlert = bootstrap.Alert.getInstance(alert);
                        if (bsAlert) {
                            bsAlert.close();
                        }
                    } else {
                        alert.remove();
                    }
                }
            }, duration);
        }
    }

    updateUIWithAnalysisResults(results) {
        // Обновляем интерфейс с результатами анализа
        console.log('Updating UI with analysis results:', results);

        // Можно добавить логику для обновления найденных элементов без перезагрузки
        if (results.found_items) {
            this.showMessage(`Analysis complete. Found ${results.found_items.total_found || 0} elements.`, 'success');
        }
    }
}

/* ============================================
   GLOBAL INITIALIZATION
   ============================================ */

document.addEventListener('DOMContentLoaded', function() {
    try {
        const analyzer = new GameAnalyzerUI();
        window.gameAnalyzer = analyzer;

        // Дополнительные обработчики
        window.addEventListener('resize', () => {
            setTimeout(() => {
                analyzer.forceTextAlignmentFix();
            }, 100);
        });

        // Обработка скрытия вкладок
        const tabLinks = document.querySelectorAll('#analyzeTabs .nav-link');
        tabLinks.forEach(link => {
            link.addEventListener('hidden.bs.tab', () => {
                setTimeout(() => {
                    analyzer.setupTooltips();
                }, 50);
            });
        });

        // Обработка перед закрытием страницы
        window.addEventListener('beforeunload', () => {
            // Сохраняем текущую вкладку перед закрытием
            analyzer.saveCurrentTab();
        });

    } catch (error) {
        console.error('Failed to initialize Game Analyzer UI:', error);

        // Fallback сообщение об ошибке
        const errorAlert = document.createElement('div');
        errorAlert.className = 'alert alert-danger alert-dismissible fade show position-fixed';
        errorAlert.style.cssText = `
            top: 20px;
            left: 50%;
            transform: translateX(-50%);
            z-index: 1060;
            min-width: 300px;
            max-width: 500px;
            text-align: center;
        `;
        errorAlert.innerHTML = `
            <i class="bi bi-x-circle me-2"></i>
            Failed to initialize analyzer. Please refresh the page.
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        document.body.appendChild(errorAlert);
    }
});