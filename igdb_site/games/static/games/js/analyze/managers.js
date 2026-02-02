// games/static/games/js/analyze/managers.js
/**
 * Менеджеры для управления вкладками, скроллом, формами и т.д.
 */

/* ============================================
   TAB MANAGEMENT
   ============================================ */

export function switchTabByName(analyzer, tabName) {
    const tabLink = document.querySelector(`#analyzeTabs a[href="#${tabName}"]`);
    if (tabLink) {
        try {
            if (window.bootstrap) {
                const tab = new bootstrap.Tab(tabLink);
                tab.show();
            } else {
                analyzer.manualTabSwitch(tabName);
            }
        } catch (error) {
            analyzer.manualTabSwitch(tabName);
        }
    }
}

export function manualTabSwitch(analyzer, tabName) {
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
        analyzer.onTabSwitch(tabName);
    }
}

export function updateUrlParam(key, value) {
    try {
        const url = new URL(window.location);
        url.searchParams.set(key, value);
        window.history.replaceState({}, '', url);
    } catch (e) {
        console.error('Failed to update URL:', e);
    }
}

export function removeUrlParam(param) {
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
   TEXT ALIGNMENT FIX
   ============================================ */

export function forceTextAlignmentFix(analyzer) {
    const activePane = document.querySelector(`#${analyzer.currentTab}.tab-pane.active`);
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
   SCROLL MANAGEMENT
   ============================================ */

export function scrollToHighlight(analyzer, elementName) {
    const activePane = document.querySelector(`#${analyzer.currentTab}.tab-pane.active`);
    if (!activePane) return;

    // Ищем все элементы с этим именем, включая множественные критерии
    const allHighlights = activePane.querySelectorAll(`
        .highlight-genre[data-element-name="${elementName}"],
        .highlight-theme[data-element-name="${elementName}"],
        .highlight-perspective[data-element-name="${elementName}"],
        .highlight-game_mode[data-element-name="${elementName}"],
        .highlight-keyword[data-element-name="${elementName}"],
        .highlight-multi[data-element-names*="${elementName}"]
    `);

    if (allHighlights.length > 0) {
        let targetHighlight = allHighlights[0];

        // Сначала убираем предыдущее специальное выделение
        const previousSpecial = activePane.querySelectorAll('.highlight-found-element');
        previousSpecial.forEach(el => {
            // ИСПРАВЛЕНИЕ: Проверяем, что элемент существует
            if (el && el.classList) {
                el.classList.remove('highlight-found-element', 'highlight-blink', 'highlight-gradient');
            }
        });

        // Прокручиваем к первому элементу внутри текстовой области
        const textDisplayArea = activePane.querySelector('.text-display-area');
        if (textDisplayArea && targetHighlight) {
            const highlightRect = targetHighlight.getBoundingClientRect();
            const areaRect = textDisplayArea.getBoundingClientRect();
            const relativeTop = highlightRect.top - areaRect.top;

            textDisplayArea.scrollTop = textDisplayArea.scrollTop + relativeTop - (areaRect.height / 2);

            // Сохраняем новую позицию прокрутки
            analyzer.saveTabScrollPosition(analyzer.currentTab);
        }

        // Применяем специальное выделение ко ВСЕМ совпадениям
        allHighlights.forEach((h, index) => {
            // ИСПРАВЛЕНИЕ: Проверяем, что элемент существует
            if (!h || !h.classList) return;

            // Добавляем классы специальной подсветки
            h.classList.add('highlight-found-element', 'highlight-blink', 'highlight-transition');

            // Добавляем градиентный эффект
            h.classList.add('highlight-gradient');

            // Добавляем анимацию с задержкой для создания волны
            setTimeout(() => {
                h.style.animation = 'highlightFoundElementPulse 2s ease-in-out';
            }, index * 50);

            // Добавляем flash анимацию
            flashElement(analyzer, h);

            // Добавляем индикатор количества совпадений
            createMatchCountIndicator(analyzer, h, allHighlights.length);
        });

        // Показываем сообщение о количестве найденных совпадений
        if (allHighlights.length > 1) {
            analyzer.showMessage(`Found ${allHighlights.length} occurrences of "${elementName}"`, 'info', 3000);
        }
    }
}

export function scrollToTop() {
    window.scrollTo({
        top: 0,
        behavior: 'smooth'
    });
}

export function handleScroll(analyzer) {
    if (!analyzer.elements.scrollToTopBtn) return;

    if (window.scrollY > 300) {
        analyzer.elements.scrollToTopBtn.classList.add('visible');
    } else {
        analyzer.elements.scrollToTopBtn.classList.remove('visible');
    }
}

/* ============================================
   UTILITY FUNCTIONS
   ============================================ */

function flashElement(analyzer, element) {
    // ИСПРАВЛЕНИЕ: Проверяем наличие элемента и его classList
    if (!element || !element.classList) return;

    element.classList.add('highlight-flash');
    setTimeout(() => {
        if (element && element.classList) {
            element.classList.remove('highlight-flash');
        }
    }, 2000);
}

function createMatchCountIndicator(analyzer, element, count) {
    // ИСПРАВЛЕНИЕ: Проверяем наличие элемента
    if (!element) return;

    // Удаляем старый индикатор, если есть
    const oldIndicator = element.querySelector('.highlight-match-count');
    if (oldIndicator && oldIndicator.parentNode) {
        oldIndicator.remove();
    }

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

/* ============================================
   MESSAGE UTILITIES
   ============================================ */

export function showMessage(text, type = 'info', duration = 5000) {
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