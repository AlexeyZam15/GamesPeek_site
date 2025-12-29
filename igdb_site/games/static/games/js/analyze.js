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
        this.currentMode = '';
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

        // Проверяем параметр cleared в URL
        const urlParams = new URLSearchParams(window.location.search);
        if (urlParams.get('cleared') === '1') {
            console.log('Results were cleared, refreshing UI...');
            this.showMessage('✅ Несохранённые результаты успешно очищены.', 'success');

            // Убираем параметр из URL без перезагрузки
            setTimeout(() => {
                this.removeUrlParam('cleared');
            }, 3000);
        }

        console.log('Game Analyzer UI initialized');
    }

    removeUrlParam(param) {
        try {
            const url = new URL(window.location);
            url.searchParams.delete(param);
            window.history.replaceState({}, '', url);
        } catch (e) {
            console.error('Failed to remove URL param:', e);
        }
    }

    cacheElements() {
        this.elements = {
            // Основные элементы
            modeButtons: document.querySelectorAll('.mode-btn'),
            analyzeForm: document.getElementById('analyze-form'),
            tabSelect: document.getElementById('analyze-tab-select'),

            // Переключатель подсветки
            highlightToggle: document.getElementById('highlight-toggle'),

            // Bootstrap табы
            analyzeTabs: document.getElementById('analyzeTabs'),
            analyzeTabLinks: document.querySelectorAll('#analyzeTabs .nav-link'),
            analyzeTabPanes: document.querySelectorAll('.tab-pane'),

            // Кнопки действий
            copyTextBtn: document.getElementById('copy-text-btn'),
            findHighlightsBtn: document.getElementById('find-highlights-btn'),
            scrollToTopBtn: document.querySelector('.scroll-to-top'),

            // Информационные элементы
            statusBar: document.querySelector('.status-bar'),
            currentModeDisplay: document.getElementById('current-mode-display'),
            currentTabDisplay: document.getElementById('current-tab-display'),
            currentTabChars: document.getElementById('current-tab-chars'),

            // Скрытые поля формы
            tabInput: document.getElementById('analyze-tab-input'),
            modeInput: document.getElementById('analyze-mode-input'),

            // Кнопки формы
            analyzeButton: document.getElementById('analyze-button'),
            saveButton: document.getElementById('save-button'),
            clearResultsBtn: document.getElementById('clear-results-btn'),
            backToGameBtn: document.getElementById('back-to-game-btn')
        };

        console.log('Elements cached:');
        console.log('- Form:', this.elements.analyzeForm ? 'found' : 'NOT FOUND');
        console.log('- Analyze button:', this.elements.analyzeButton ? 'found' : 'NOT FOUND');
        console.log('- Tab select:', this.elements.tabSelect ? 'found' : 'NOT FOUND');
        console.log('- Mode buttons:', this.elements.modeButtons.length);
    }

    getCurrentState() {
        // Получаем текущую активную вкладку
        const activeTabLink = document.querySelector('#analyzeTabs .nav-link.active');
        if (activeTabLink) {
            const href = activeTabLink.getAttribute('href');
            this.currentTab = href ? href.substring(1) : '';
        } else if (this.elements.analyzeTabLinks.length > 0) {
            const firstTab = this.elements.analyzeTabLinks[0];
            const href = firstTab.getAttribute('href');
            this.currentTab = href ? href.substring(1) : '';
        }

        // Получаем текущий режим
        const activeModeButton = document.querySelector('.mode-btn.active');
        if (activeModeButton) {
            this.currentMode = activeModeButton.dataset.mode;
        } else {
            this.currentMode = 'criteria';
        }
    }

    /* ============================================
       EVENT HANDLING METHODS
       ============================================ */

    forceFormSubmit(buttonName) {
        console.log(`forceFormSubmit called for button: ${buttonName}`);

        if (!this.elements.analyzeForm) {
            console.error('Cannot submit: form not found');
            return false;
        }

        // Создаем скрытое поле с именем кнопки
        const hiddenInput = document.createElement('input');
        hiddenInput.type = 'hidden';
        hiddenInput.name = buttonName;
        hiddenInput.value = '1';
        this.elements.analyzeForm.appendChild(hiddenInput);

        console.log('Submitting form with hidden input:', hiddenInput.name);

        // Отправляем форму
        try {
            this.elements.analyzeForm.submit();
            return true;
        } catch (error) {
            console.error('Error submitting form:', error);
            return false;
        }
    }

    debugFormState() {
        console.log('=== DEBUG FORM STATE ===');

        if (!this.elements.analyzeForm) {
            console.error('Form not found');
            return;
        }

        // Выводим все поля формы
        const formData = new FormData(this.elements.analyzeForm);
        console.log('Form fields:');
        for (let [name, value] of formData.entries()) {
            console.log(`  ${name}: ${value}`);
        }

        // Проверяем типы кнопок
        const buttons = this.elements.analyzeForm.querySelectorAll('button');
        console.log(`Found ${buttons.length} buttons in form:`);
        buttons.forEach((btn, index) => {
            console.log(`  Button ${index}: name="${btn.name}", type="${btn.type}", text="${btn.textContent.trim()}"`);
        });

        console.log('=== END FORM DEBUG ===');
    }

    debugFormState() {
        console.log('=== DEBUG FORM STATE ===');

        if (!this.elements.analyzeForm) {
            console.error('Form not found');
            return;
        }

        // Выводим все поля формы
        const formData = new FormData(this.elements.analyzeForm);
        console.log('Form fields:');
        for (let [name, value] of formData.entries()) {
            console.log(`  ${name}: ${value}`);
        }

        // Проверяем типы кнопок
        const buttons = this.elements.analyzeForm.querySelectorAll('button');
        console.log(`Found ${buttons.length} buttons in form:`);
        buttons.forEach((btn, index) => {
            console.log(`  Button ${index}: name="${btn.name}", type="${btn.type}", text="${btn.textContent.trim()}"`);
        });

        console.log('=== END FORM DEBUG ===');
    }

    updateHiddenFields() {
        console.log('=== UPDATING HIDDEN FIELDS ===');

        // Обновляем поле вкладки из селекта
        if (this.elements.tabSelect && this.elements.tabInput) {
            const selectedTab = this.elements.tabSelect.value;
            this.elements.tabInput.value = selectedTab;
            console.log(`Tab input updated: ${selectedTab} → ${this.elements.tabInput.value}`);
        } else {
            console.error('Tab select or input not found');
            if (!this.elements.tabSelect) console.error('- Tab select missing');
            if (!this.elements.tabInput) console.error('- Tab input missing');
        }

        // Обновляем поле режима из активной кнопки
        const activeModeBtn = document.querySelector('.mode-btn.active');
        if (activeModeBtn && this.elements.modeInput) {
            const mode = activeModeBtn.dataset.mode || 'criteria';
            this.elements.modeInput.value = mode;
            console.log(`Mode input updated: ${mode} → ${this.elements.modeInput.value}`);
        } else {
            console.warn('Active mode button or mode input not found');
            console.log('- Active button:', activeModeBtn);
            console.log('- Mode input:', this.elements.modeInput);

            // Устанавливаем значение по умолчанию
            if (this.elements.modeInput) {
                this.elements.modeInput.value = 'criteria';
                console.log('Mode set to default: criteria');
            }
        }

        // Убедимся, что все обязательные поля формы присутствуют
        const requiredNames = ['csrfmiddlewaretoken', 'analyze_tab', 'analyze_mode'];
        requiredNames.forEach(name => {
            const field = this.elements.analyzeForm.querySelector(`[name="${name}"]`);
            if (!field) {
                console.error(`Missing required field: ${name}`);
            } else {
                console.log(`Field ${name} found with value: ${field.value}`);
            }
        });

        console.log('Hidden fields update complete');
    }

    bindEvents() {
        console.log('=== BINDING EVENTS ===');

        // 1. Переключатели режимов
        this.bindModeButtons();

        // 2. Селект вкладок
        this.bindTabSelect();

        // 3. Переключатель подсветки
        this.bindHighlightToggle();

        // 4. Кнопка анализа - САМАЯ ВАЖНАЯ!
        this.bindAnalyzeButton();

        // 5. Кнопка сохранения
        this.bindSaveButton();

        // 6. Прочие кнопки
        this.bindOtherButtons();

        // 7. Табы Bootstrap
        this.bindBootstrapTabs();

        // 8. Добавляем обработчик на саму форму для отладки
        if (this.elements.analyzeForm) {
            this.elements.analyzeForm.addEventListener('submit', (e) => {
                console.log('=== FORM SUBMIT EVENT FIRED ===');
                console.log('Submit triggered by:', e.submitter ? e.submitter.name : 'unknown');
                console.log('Form data:');
                const formData = new FormData(this.elements.analyzeForm);
                for (let [name, value] of formData.entries()) {
                    console.log(`  ${name}: ${value}`);
                }
            });
        }

        console.log('=== ALL EVENTS BOUND ===');
    }

    bindClearResultsButton() {
        const clearButton = document.getElementById('clear-results-btn');
        if (!clearButton) {
            console.log('Clear results button not found');
            return;
        }

        console.log('Found clear results button:', clearButton);

        // Клонируем кнопку для чистого состояния
        const originalButton = clearButton;
        const newButton = originalButton.cloneNode(true);
        originalButton.parentNode.replaceChild(newButton, originalButton);
        const clearBtn = newButton;

        clearBtn.addEventListener('click', async (e) => {
            e.preventDefault();
            e.stopPropagation();

            if (!confirm('Вы уверены, что хотите очистить несохранённые результаты анализа?\n\nЭто действие нельзя отменить.')) {
                console.log('Clear cancelled by user');
                return;
            }

            // Показываем спиннер
            const originalHTML = clearBtn.innerHTML;
            clearBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Clearing...';
            clearBtn.disabled = true;

            console.log('Clearing analysis results...');

            // Получаем URL из href атрибута
            const clearUrl = clearBtn.getAttribute('href');

            if (!clearUrl) {
                console.error('Clear URL not found');
                this.showMessage('Error: Clear URL not found', 'error');
                this.resetClearButton(clearBtn, originalHTML);
                return;
            }

            console.log('Clear URL:', clearUrl);

            try {
                // Используем Fetch API для очистки результатов
                const response = await fetch(clearUrl, {
                    method: 'GET',
                    headers: {
                        'X-Requested-With': 'XMLHttpRequest',
                        'Accept': 'application/json',
                    },
                    credentials: 'same-origin'
                });

                console.log('Clear response status:', response.status);

                if (response.ok) {
                    const data = await response.json();
                    console.log('Clear response data:', data);

                    if (data.success) {
                        this.showMessage('✅ ' + data.message, 'success');

                        // Если есть redirect_url, используем его
                        if (data.redirect_url) {
                            setTimeout(() => {
                                window.location.href = data.redirect_url;
                            }, 1500);
                        } else {
                            // Иначе просто перезагружаем страницу
                            setTimeout(() => {
                                window.location.reload();
                            }, 1500);
                        }
                    } else {
                        throw new Error(data.message || 'Ошибка очистки');
                    }
                } else {
                    throw new Error(`Ошибка HTTP: ${response.status}`);
                }
            } catch (error) {
                console.error('Error clearing results:', error);
                this.showMessage(`❌ Ошибка при очистке: ${error.message}`, 'error');
                this.resetClearButton(clearBtn, originalHTML);
            }
        });

        console.log('Clear results button bound successfully');
    }

    resetClearButton(button, originalHTML) {
        if (button) {
            button.innerHTML = originalHTML;
            button.disabled = false;
        }
    }

    bindModeButtons() {
        if (!this.elements.modeButtons || this.elements.modeButtons.length === 0) {
            console.error('Mode buttons not found');
            return;
        }

        this.elements.modeButtons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                console.log(`Mode button clicked: ${btn.dataset.mode}`);
                this.switchMode(e);
            });
        });
    }

    bindTabSelect() {
        if (!this.elements.tabSelect) {
            console.error('Tab select not found');
            return;
        }

        this.elements.tabSelect.addEventListener('change', (e) => {
            const tabName = e.target.value;
            console.log(`Tab select changed to: ${tabName}`);

            // Обновляем скрытое поле
            if (this.elements.tabInput) {
                this.elements.tabInput.value = tabName;
            }

            // Переключаем вкладку
            this.switchTabByName(tabName);
        });
    }

    bindHighlightToggle() {
        if (!this.elements.highlightToggle) {
            console.warn('Highlight toggle not found');
            return;
        }

        this.elements.highlightToggle.addEventListener('change', (e) => {
            console.log(`Highlight toggle: ${e.target.checked}`);
            this.handleHighlightToggle(e);
        });
    }

    bindAnalyzeButton() {
        console.log('=== BINDING ANALYZE BUTTON ===');

        if (!this.elements.analyzeButton) {
            console.error('ANALYZE BUTTON NOT FOUND! This is critical.');
            this.showMessage('Error: Analyze button not found. Please refresh page.', 'error');
            return;
        }

        console.log('Found analyze button:', this.elements.analyzeButton);

        // Создаем клон кнопки для чистого состояния
        const originalButton = this.elements.analyzeButton;
        const newButton = originalButton.cloneNode(true);

        // Заменяем старую кнопку новой
        originalButton.parentNode.replaceChild(newButton, originalButton);
        this.elements.analyzeButton = newButton;

        console.log('Button cloned and replaced');

        // Добавляем обработчик на новую кнопку
        this.elements.analyzeButton.addEventListener('click', (e) => {
            console.log('=== ANALYZE BUTTON CLICKED ===');
            console.log('Event details:', e);
            console.log('Button name:', this.elements.analyzeButton.name);
            console.log('Button type:', this.elements.analyzeButton.type);

            // ПРЕДОТВРАЩАЕМ СТАНДАРТНОЕ ПОВЕДЕНИЕ
            e.preventDefault();
            e.stopPropagation();

            // Проверяем наличие формы
            if (!this.elements.analyzeForm) {
                console.error('Form element not found');
                this.showMessage('Error: Form not found', 'error');
                return;
            }

            console.log('Form found:', this.elements.analyzeForm);

            // Обновляем скрытые поля
            this.updateHiddenFields();

            // Проверяем CSRF токен
            const csrfToken = document.querySelector('[name="csrfmiddlewaretoken"]');
            if (!csrfToken) {
                console.error('CSRF token not found');
                this.showMessage('Error: Security token missing. Please refresh page.', 'error');
                return;
            }

            console.log('CSRF token found');

            // Убедимся, что в форме есть поле analyze
            let analyzeField = this.elements.analyzeForm.querySelector('input[name="analyze"]');
            if (!analyzeField) {
                console.log('Creating hidden analyze field');
                analyzeField = document.createElement('input');
                analyzeField.type = 'hidden';
                analyzeField.name = 'analyze';
                analyzeField.value = 'true';
                this.elements.analyzeForm.appendChild(analyzeField);
            }

            // Показываем спиннер
            const originalHTML = this.elements.analyzeButton.innerHTML;
            this.elements.analyzeButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Analyzing...';
            this.elements.analyzeButton.disabled = true;

            console.log('=== SUBMITTING FORM ===');
            console.log('- Form action:', this.elements.analyzeForm.action);
            console.log('- Form method:', this.elements.analyzeForm.method);
            console.log('- Tab value:', this.elements.tabInput ? this.elements.tabInput.value : 'N/A');
            console.log('- Mode value:', this.elements.modeInput ? this.elements.modeInput.value : 'N/A');
            console.log('- Has CSRF:', !!csrfToken);

            // Даем небольшую задержку для отображения спиннера
            setTimeout(() => {
                try {
                    // Отправляем форму
                    console.log('Attempting form submission...');
                    this.elements.analyzeForm.submit();
                } catch (error) {
                    console.error('Error submitting form:', error);
                    this.showMessage('Error submitting form: ' + error.message, 'error');

                    // Восстанавливаем кнопку
                    this.elements.analyzeButton.innerHTML = originalHTML;
                    this.elements.analyzeButton.disabled = false;
                }
            }, 300);
        });

        console.log('Analyze button bound successfully');
    }

    bindSaveButton() {
        if (!this.elements.saveButton) {
            console.log('Save button not found (may be hidden)');
            return;
        }

        const newButton = this.elements.saveButton.cloneNode(true);
        this.elements.saveButton.parentNode.replaceChild(newButton, this.elements.saveButton);
        this.elements.saveButton = newButton;

        this.elements.saveButton.addEventListener('click', (e) => {
            e.preventDefault();
            this.handleSaveResults(e);
        });
    }

    bindOtherButtons() {
        // Копирование текста
        if (this.elements.copyTextBtn) {
            this.elements.copyTextBtn.addEventListener('click', () => this.copyText());
        }

        // Поиск подсветки
        if (this.elements.findHighlightsBtn) {
            this.elements.findHighlightsBtn.addEventListener('click', () => this.scrollToFirstHighlight());
        }

        // Прокрутка к верху
        if (this.elements.scrollToTopBtn) {
            this.elements.scrollToTopBtn.addEventListener('click', () => this.scrollToTop());
            window.addEventListener('scroll', () => this.handleScroll());
        }

        // Кнопка очистки результатов
        this.bindClearResultsButton(); // ДОБАВЬТЕ ЭТУ СТРОЧКУ

        // Клик по найденным элементам
        document.addEventListener('click', (e) => {
            if (e.target.classList.contains('found-item-badge')) {
                e.preventDefault();
                this.scrollToHighlight(e.target.dataset.name);
            }
        });
    }

    bindBootstrapTabs() {
        if (!this.elements.analyzeTabLinks || this.elements.analyzeTabLinks.length === 0) {
            return;
        }

        this.elements.analyzeTabLinks.forEach(link => {
            link.addEventListener('shown.bs.tab', (e) => {
                const href = e.target.getAttribute('href');
                if (href) {
                    const tabName = href.substring(1);
                    this.onTabSwitch(tabName);
                }
            });
        });
    }

    resetAnalyzeButton(originalHTML) {
        if (this.elements.analyzeButton) {
            this.elements.analyzeButton.innerHTML = originalHTML;
            this.elements.analyzeButton.disabled = false;
        }
    }

    validateElements() {
        const requiredElements = [
            { name: 'analyzeForm', element: this.elements.analyzeForm },
            { name: 'modeInput', element: this.elements.modeInput },
            { name: 'tabInput', element: this.elements.tabInput },
        ];

        const missing = requiredElements.filter(item => !item.element);
        if (missing.length > 0) {
            console.error('Missing required elements:', missing.map(item => item.name));
            this.showMessage(`Missing elements: ${missing.map(item => item.name).join(', ')}`, 'error');
        }
    }

    handleHighlightToggle(e) {
        console.log(`handleHighlightToggle: ${e.target.checked ? 'enabling' : 'disabling'} highlighting`);

        e.preventDefault();
        e.stopPropagation();

        const isEnabled = e.target.checked;

        // Показываем сообщение
        this.showMessage(isEnabled ? 'Highlighting enabled...' : 'Highlighting disabled...', 'info');

        if (this.elements.analyzeForm) {
            // Создаем скрытое поле для отслеживания изменения переключателя
            let toggleInput = this.elements.analyzeForm.querySelector('input[name="highlight_toggle_only"]');
            if (!toggleInput) {
                toggleInput = document.createElement('input');
                toggleInput.type = 'hidden';
                toggleInput.name = 'highlight_toggle_only';
                this.elements.analyzeForm.appendChild(toggleInput);
            }
            toggleInput.value = '1';

            // Устанавливаем значение переключателя
            let highlightInput = this.elements.analyzeForm.querySelector('input[name="highlight_toggle"]');
            if (!highlightInput) {
                highlightInput = document.createElement('input');
                highlightInput.type = 'hidden';
                highlightInput.name = 'highlight_toggle';
                this.elements.analyzeForm.appendChild(highlightInput);
            }
            highlightInput.value = isEnabled ? 'on' : 'off';

            console.log('Toggle fields added to form');

            // Не отправляем форму автоматически - пусть пользователь нажмет "Analyze"
            // Вместо этого обновляем сессию через AJAX или просто сохраняем состояние
            this.showMessage('Now click "Analyze Text" to apply highlighting changes', 'info');
        } else {
            console.error('Cannot handle highlight toggle: form not found');
            this.showMessage('Error: Cannot toggle highlighting. Form not found.', 'error');
        }
    }

    /* ============================================
       TAB & MODE MANAGEMENT
       ============================================ */

    loadUrlParams() {
        const urlParams = new URLSearchParams(window.location.search);
        const tab = urlParams.get('tab');
        const mode = urlParams.get('mode');

        if (tab) {
            this.switchTabByName(tab);
        }

        if (mode) {
            const modeButton = document.querySelector(`.mode-btn[data-mode="${mode}"]`);
            if (modeButton) {
                modeButton.click();
            }
        }
    }

    switchMode(e) {
        const button = e.currentTarget;
        const mode = button.dataset.mode;

        if (this.currentMode === mode) return;

        console.log(`Switching to mode: ${mode}`);

        // Обновляем активную кнопку
        this.elements.modeButtons.forEach(btn => {
            btn.classList.remove('active');
            btn.setAttribute('aria-pressed', 'false');
        });
        button.classList.add('active');
        button.setAttribute('aria-pressed', 'true');

        // Обновляем скрытое поле
        if (this.elements.modeInput) {
            this.elements.modeInput.value = mode;
            console.log(`Mode input set to: ${this.elements.modeInput.value}`);
        }

        // Обновляем отображение
        if (this.elements.currentModeDisplay) {
            this.elements.currentModeDisplay.textContent =
                mode === 'criteria' ? 'Criteria Analysis' : 'Keyword Analysis';
        }

        this.currentMode = mode;

        // Обновляем URL
        this.updateUrlParam('mode', mode);

        // Показываем сообщение
        this.showMessage(`Switched to ${mode} analysis mode. Click "Analyze Text" to analyze.`, 'info');
    }

    switchTabByName(tabName) {
        console.log(`switchTabByName called with: ${tabName}`);

        const tabLink = document.querySelector(`#analyzeTabs a[href="#${tabName}"]`);
        if (tabLink) {
            console.log(`Found tab link for: ${tabName}`);

            // Используем Bootstrap Tab API
            try {
                const tab = new bootstrap.Tab(tabLink);
                tab.show();
                console.log(`Tab ${tabName} shown successfully`);
            } catch (error) {
                console.error('Error showing tab:', error);
                // Fallback: переключаем вручную
                this.manualTabSwitch(tabName);
            }
        } else {
            console.error(`Tab link not found for: ${tabName}`);
            this.showMessage(`Error: Tab ${tabName} not found.`, 'error');
        }
    }

    onTabSwitch(tabName) {
        if (this.currentTab === tabName) return;

        // Синхронизируем выпадающий список
        if (this.elements.tabSelect) {
            this.elements.tabSelect.value = tabName;
        }

        // Обновляем скрытое поле
        if (this.elements.tabInput) {
            this.elements.tabInput.value = tabName;
        }

        // Обновляем отображение
        this.updateTabDisplay(tabName);

        this.currentTab = tabName;

        // Исправляем выравнивание
        setTimeout(() => {
            this.forceTextAlignmentFix();
            this.setupTooltips();
        }, 10);

        // Обновляем URL
        this.updateUrlParam('tab', tabName);
    }

    updateTabDisplay(tabName) {
        if (this.elements.currentTabDisplay && this.elements.currentTabChars) {
            const tabLink = document.querySelector(`#analyzeTabs a[href="#${tabName}"]`);
            if (tabLink) {
                const label = tabLink.textContent.match(/[^\(]+/)[0].trim();
                const chars = tabLink.querySelector('.badge').textContent;

                this.elements.currentTabDisplay.textContent = label;
                this.elements.currentTabChars.textContent = chars;
            }
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

        // Сбрасываем все стили
        textContent.style.cssText = `
            text-align: left !important;
            margin: 0 !important;
            padding: 0 !important;
            position: static !important;
            transform: none !important;
            display: block !important;
            vertical-align: top !important;
        `;

        // Исправляем параграфы
        const paragraphs = textContent.querySelectorAll('p');
        paragraphs.forEach(p => {
            p.style.cssText = `
                text-align: left !important;
                margin-bottom: 1.25em !important;
                margin-top: 0 !important;
            `;
        });

        // Убираем центрирование у всех заголовков
        const headings = textContent.querySelectorAll('h1, h2, h3, h4, h5, h6');
        headings.forEach(h => {
            h.style.cssText = 'text-align: left !important;';
        });

        // Убираем центрирование у всех div элементов
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

    copyText() {
        const activePane = document.querySelector(`#${this.currentTab}.tab-pane.active`);
        if (!activePane) {
            this.showMessage('No text available to copy', 'warning');
            return;
        }

        const textContent = activePane.querySelector('.text-content');
        if (!textContent) {
            this.showMessage('No text available to copy', 'warning');
            return;
        }

        // Клонируем и очищаем HTML
        const clone = textContent.cloneNode(true);

        // Удаляем информацию о подсветке
        clone.querySelectorAll('.highlight-info').forEach(el => el.remove());

        // Удаляем подсветку
        clone.querySelectorAll('mark').forEach(mark => {
            mark.replaceWith(mark.textContent);
        });

        const text = clone.textContent || clone.innerText;

        if (!text.trim()) {
            this.showMessage('No text available to copy', 'warning');
            return;
        }

        navigator.clipboard.writeText(text.trim())
            .then(() => {
                this.showMessage('Text copied to clipboard!', 'success');
            })
            .catch(err => {
                console.error('Clipboard error:', err);
                this.showMessage('Failed to copy text', 'error');
            });
    }

    scrollToHighlight(elementName) {
        const activePane = document.querySelector(`#${this.currentTab}.tab-pane.active`);
        if (!activePane) return;

        const highlights = activePane.querySelectorAll(`[data-element-name="${elementName}"]`);
        if (highlights.length > 0) {
            // Находим первое видимое выделение
            let targetHighlight = highlights[0];
            for (let i = 0; i < highlights.length; i++) {
                const rect = highlights[i].getBoundingClientRect();
                if (rect.top >= 0 && rect.bottom <= window.innerHeight) {
                    targetHighlight = highlights[i];
                    break;
                }
            }

            targetHighlight.scrollIntoView({
                behavior: 'smooth',
                block: 'center'
            });

            highlights.forEach(h => {
                h.classList.add('highlight-pulse');
                setTimeout(() => h.classList.remove('highlight-pulse'), 2000);
            });
        }
    }

    scrollToFirstHighlight() {
        const activePane = document.querySelector(`#${this.currentTab}.tab-pane.active`);
        if (!activePane) return;

        const firstHighlight = activePane.querySelector('mark');
        if (firstHighlight) {
            firstHighlight.scrollIntoView({
                behavior: 'smooth',
                block: 'center'
            });

            firstHighlight.classList.add('highlight-pulse');
            setTimeout(() => {
                firstHighlight.classList.remove('highlight-pulse');
            }, 2000);
        } else {
            this.showMessage('No highlighted elements found', 'info');
        }
    }

    scrollToTop() {
        window.scrollTo({
            top: 0,
            behavior: 'smooth'
        });
    }

    handleSaveResults(e) {
        console.log('handleSaveResults called');
        e.preventDefault();

        if (!confirm('Are you sure you want to save the analysis results to the game database?\n\nThis will add new elements (genres, themes, keywords, etc.) to the game.')) {
            console.log('Save cancelled by user');
            return;
        }

        if (!this.elements.analyzeForm) {
            console.error('Cannot save: form not found');
            this.showMessage('Error: Form not found. Cannot save results.', 'error');
            return;
        }

        console.log('Starting save process...');

        // Показываем спиннер на кнопке сохранения
        const saveButton = this.elements.analyzeForm.querySelector('button[name="save_results"]');
        if (saveButton) {
            const originalHTML = saveButton.innerHTML;
            saveButton.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Saving...';
            saveButton.disabled = true;
            saveButton.dataset.originalHTML = originalHTML;
        }

        this.showMessage('Saving results to database...', 'info');

        // Используем простую отправку формы
        console.log('Creating hidden input for save_results...');

        // Создаем скрытое поле для save_results
        const saveInput = document.createElement('input');
        saveInput.type = 'hidden';
        saveInput.name = 'save_results';
        saveInput.value = 'true';
        this.elements.analyzeForm.appendChild(saveInput);

        console.log('Submitting form for save...');

        // Отправляем форму
        setTimeout(() => {
            try {
                this.elements.analyzeForm.submit();
                console.log('Form submitted successfully for save');
            } catch (error) {
                console.error('Error submitting form for save:', error);
                this.showMessage('❌ Error submitting form: ' + error.message, 'error');

                // Восстанавливаем кнопку
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
        // Инициализируем тултипы Bootstrap
        const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
        tooltipTriggerList.map(tooltipTriggerEl => {
            // Удаляем существующие тултипы
            const existingTooltip = bootstrap.Tooltip.getInstance(tooltipTriggerEl);
            if (existingTooltip) {
                existingTooltip.dispose();
            }
            // Создаем новые тултипы
            return new bootstrap.Tooltip(tooltipTriggerEl, {
                trigger: 'hover focus'
            });
        });

        // Добавляем тултипы для подсвеченных элементов
        const activePane = document.querySelector(`#${this.currentTab}.tab-pane.active`);
        if (activePane) {
            const highlights = activePane.querySelectorAll('mark');
            highlights.forEach(highlight => {
                if (!highlight.hasAttribute('data-bs-toggle')) {
                    const elementName = highlight.dataset.elementName || 'Found element';
                    highlight.setAttribute('data-bs-toggle', 'tooltip');
                    highlight.setAttribute('data-bs-title', elementName);
                    highlight.setAttribute('data-bs-placement', 'top');

                    // Удаляем существующий тултип, если есть
                    const existingTooltip = bootstrap.Tooltip.getInstance(highlight);
                    if (existingTooltip) {
                        existingTooltip.dispose();
                    }
                    new bootstrap.Tooltip(highlight, {
                        trigger: 'hover focus',
                        placement: 'top'
                    });
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

        if (this.elements.statusBar) {
            if (window.scrollY > 100) {
                this.elements.statusBar.classList.add('visible');
            } else {
                this.elements.statusBar.classList.remove('visible');
            }
        }
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

    disableFormButtons() {
        console.log('Disabling form buttons...');

        // Отключаем все кнопки submit в форме
        if (this.elements.analyzeForm) {
            const formButtons = this.elements.analyzeForm.querySelectorAll('button[type="submit"]');
            console.log(`Found ${formButtons.length} submit buttons`);

            formButtons.forEach((button, index) => {
                console.log(`Button ${index + 1}: name="${button.name}", text="${button.textContent.trim()}"`);

                // Сохраняем оригинальный текст и состояние
                if (!button.dataset.originalText) {
                    button.dataset.originalText = button.innerHTML;
                }
                if (!button.dataset.wasDisabled) {
                    button.dataset.wasDisabled = button.disabled;
                }

                // Отключаем кнопку и показываем спиннер
                button.disabled = true;

                if (button.name === 'analyze') {
                    button.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Analyzing...';
                    console.log('Analyze button disabled and showing spinner');
                } else if (button.name === 'save_results') {
                    button.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Processing...';
                    console.log('Save results button disabled and showing spinner');
                }
            });

            // Включаем кнопки через 30 секунд (на случай если что-то пошло не так)
            setTimeout(() => {
                this.enableFormButtons();
            }, 30000);
        } else {
            console.warn('Cannot disable buttons: form not found');
        }
    }

    enableFormButtons() {
        console.log('Enabling form buttons...');

        if (this.elements.analyzeForm) {
            const formButtons = this.elements.analyzeForm.querySelectorAll('button[type="submit"]');

            formButtons.forEach(button => {
                // Восстанавливаем оригинальный текст
                if (button.dataset.originalText) {
                    button.innerHTML = button.dataset.originalText;
                }

                // Восстанавливаем оригинальное состояние disabled
                if (button.dataset.wasDisabled) {
                    button.disabled = (button.dataset.wasDisabled === 'true');
                } else {
                    button.disabled = false;
                }
            });

            console.log(`Enabled ${formButtons.length} buttons`);
        }
    }

    showLoading(message = 'Processing...') {
        if (this.elements.statusBar) {
            this.elements.statusBar.classList.add('visible');
            const statusMessage = this.elements.statusBar.querySelector('.status-message');
            if (statusMessage) {
                statusMessage.innerHTML =
                    `<i class="bi bi-hourglass-split text-warning me-2"></i> ${message}`;
            }
        }
    }

    hideLoading() {
        if (this.elements.statusBar) {
            setTimeout(() => {
                this.elements.statusBar.classList.remove('visible');
            }, 1000);
        }
    }

    showMessage(text, type = 'info') {
        // Удаляем старые сообщения
        const oldAlerts = document.querySelectorAll('.analyzer-alert');
        oldAlerts.forEach(alert => {
            const bsAlert = bootstrap.Alert.getInstance(alert);
            if (bsAlert) {
                bsAlert.close();
            } else {
                alert.remove();
            }
        });

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
        new bootstrap.Alert(alert);

        // Автоматически закрываем через 5 секунд (кроме ошибок)
        if (type !== 'error') {
            setTimeout(() => {
                if (alert.parentNode) {
                    const bsAlert = bootstrap.Alert.getInstance(alert);
                    if (bsAlert) {
                        bsAlert.close();
                    }
                }
            }, 5000);
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

        // Принудительное исправление выравнивания при загрузке
        setTimeout(() => {
            analyzer.forceTextAlignmentFix();
        }, 100);

        // Добавляем обработку изменения размера окна
        window.addEventListener('resize', () => {
            setTimeout(() => {
                analyzer.forceTextAlignmentFix();
            }, 100);
        });

        // Добавляем обработку закрытия табов
        const tabLinks = document.querySelectorAll('#analyzeTabs .nav-link');
        tabLinks.forEach(link => {
            link.addEventListener('hidden.bs.tab', () => {
                setTimeout(() => {
                    analyzer.setupTooltips();
                }, 50);
            });
        });

    } catch (error) {
        console.error('Failed to initialize Game Analyzer UI:', error);
        // Показываем сообщение об ошибке пользователю
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

/* ============================================
   СТИЛИ ДЛЯ АНИМАЦИЙ
   ============================================ */

// Добавляем стили для анимаций
const style = document.createElement('style');
style.textContent = `
    @keyframes slideInRight {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }

    @keyframes highlightPulse {
        0% {
            box-shadow: 0 0 0 0 rgba(255, 107, 53, 0.7);
        }
        70% {
            box-shadow: 0 0 0 10px rgba(255, 107, 53, 0);
        }
        100% {
            box-shadow: 0 0 0 0 rgba(255, 107, 53, 0);
        }
    }

    .highlight-pulse {
        animation: highlightPulse 1s ease;
    }

    .scroll-to-top {
        opacity: 0;
        transition: opacity 0.3s ease, transform 0.3s ease;
    }

    .scroll-to-top.visible {
        opacity: 1;
    }

    .status-bar {
        transition: transform 0.3s ease;
    }
`;
document.head.appendChild(style);