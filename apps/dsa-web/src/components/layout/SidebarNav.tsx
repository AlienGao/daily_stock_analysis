import React, { useEffect, useState } from 'react';
import { Activity, ArrowUpRight, BarChart3, Brain, Compass, Home, LogOut, MessageSquareQuote, Search, Settings2, Sliders, TrendingUp, Users } from 'lucide-react';
import { NavLink } from 'react-router-dom';
import { ALPHASIFT_CONFIG_CHANGED_EVENT, SYSTEM_CONFIG_CHANGED_EVENT, alphasiftApi } from '../../api/alphasift';
import { useAuth } from '../../contexts/AuthContext';
import { useAgentChatStore } from '../../stores/agentChatStore';
import { useUiLanguage } from '../../contexts/UiLanguageContext';
import type { UiTextKey } from '../../i18n/uiText';
import { cn } from '../../utils/cn';
import { ConfirmDialog } from '../common/ConfirmDialog';
import { StatusDot } from '../common/StatusDot';
import { ThemeToggle } from '../theme/ThemeToggle';

type SidebarNavProps = {
  collapsed?: boolean;
  onNavigate?: () => void;
  variant?: 'default' | 'rail';
};

type NavItem = {
  key: string;
  label?: string;
  labelKey?: UiTextKey;
  to: string;
  icon: React.ComponentType<{ className?: string }>;
  exact?: boolean;
  badge?: 'completion';
};

const NAV_ITEMS: NavItem[] = [
  { key: 'home', labelKey: 'layout.nav.home', to: '/', icon: Home, exact: true },
  { key: 'discovery', label: '寻股', to: '/discovery', icon: Compass, exact: true },
  { key: 'factor-backtest', label: '因子', to: '/factor-backtest', icon: Activity },
  { key: 'factor-tuning', label: '调优', to: '/factor-tuning', icon: Sliders },
  { key: 'broker-recommend', label: '金股', to: '/broker-recommend', icon: TrendingUp },
  { key: 'new-highs', label: '新高', to: '/new-highs', icon: ArrowUpRight },
  { key: 'simple-factor-backtest', label: '快测', to: '/simple-factor-backtest', icon: BarChart3 },
  { key: 'lgb', label: 'LGB', to: '/lgb', icon: Brain },
  { key: 'institution-survey', label: '调研', to: '/institution-survey', icon: Users },
  { key: 'chat', labelKey: 'layout.nav.chat', to: '/chat', icon: MessageSquareQuote, badge: 'completion' },
  { key: 'screening', labelKey: 'layout.nav.screening', to: '/screening', icon: Search },
  { key: 'decision-signals', labelKey: 'layout.nav.decisionSignals', to: '/decision-signals', icon: Activity },
  { key: 'settings', labelKey: 'layout.nav.settings', to: '/settings', icon: Settings2 },
];

export const SidebarNav: React.FC<SidebarNavProps> = ({ collapsed = false, onNavigate, variant = 'default' }) => {
  const { authEnabled, logout } = useAuth();
  const { t } = useUiLanguage();
  const completionBadge = useAgentChatStore((state) => state.completionBadge);
  const [showLogoutConfirm, setShowLogoutConfirm] = useState(false);
  const [showAlphaSiftNav, setShowAlphaSiftNav] = useState(false);

  useEffect(() => {
    let active = true;

    const refreshAlphaSiftStatus = async () => {
      try {
        const status = await alphasiftApi.getStatus();
        if (active) {
          setShowAlphaSiftNav(status.enabled);
        }
      } catch {
        if (active) {
          setShowAlphaSiftNav(false);
        }
      }
    };

    void refreshAlphaSiftStatus();
    window.addEventListener(ALPHASIFT_CONFIG_CHANGED_EVENT, refreshAlphaSiftStatus);
    window.addEventListener(SYSTEM_CONFIG_CHANGED_EVENT, refreshAlphaSiftStatus);

    return () => {
      active = false;
      window.removeEventListener(ALPHASIFT_CONFIG_CHANGED_EVENT, refreshAlphaSiftStatus);
      window.removeEventListener(SYSTEM_CONFIG_CHANGED_EVENT, refreshAlphaSiftStatus);
    };
  }, []);

  const navItems = showAlphaSiftNav ? NAV_ITEMS : NAV_ITEMS.filter((item) => item.key !== 'screening');
  const isRail = variant === 'rail';
  const itemBaseClass = cn(
    'group relative w-full overflow-hidden rounded-xl border border-transparent text-secondary-text transition-all cursor-pointer',
    isRail
      ? 'flex min-h-[3.25rem] flex-col items-center justify-center gap-0.5 px-1 py-2 text-[10px] leading-tight'
      : cn(
          'flex h-[var(--nav-item-height)] items-center text-sm leading-none',
          collapsed ? 'justify-center px-0' : 'gap-3 px-[var(--nav-item-padding-x)]',
        ),
  );
  const itemInteractiveClass = cn(
    itemBaseClass,
    'hover:bg-[var(--nav-hover-bg)] hover:text-foreground'
  );
  const itemActiveClass = cn(
    'bg-[var(--nav-active-bg)] font-medium text-[hsl(var(--primary))]',
    isRail ? 'shadow-[inset_0_0_0_1px_var(--nav-active-border)]' : 'border-[var(--nav-active-border)]',
  );
  const itemIconClass = cn(isRail ? 'h-[18px] w-[18px]' : 'h-5 w-5', 'shrink-0');
  const itemLabelClass = cn(
    'max-w-full truncate',
    isRail ? 'text-center text-[10px] font-medium leading-tight' : '',
  );

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div
        className={cn(
          'flex shrink-0 items-center',
          isRail ? 'mb-3 justify-center pt-0.5' : 'mb-4 gap-2 px-1',
          collapsed && !isRail ? 'justify-center' : '',
        )}
      >
        <div
          className={cn(
            'flex items-center justify-center bg-primary-gradient text-[hsl(var(--primary-foreground))] shadow-[0_12px_28px_var(--nav-brand-shadow)]',
            isRail ? 'h-8 w-8 rounded-[0.85rem]' : 'h-10 w-10 rounded-2xl',
          )}
        >
          <BarChart3 className={cn(isRail ? 'h-4 w-4' : 'h-5 w-5')} />
        </div>
        {!collapsed && !isRail ? (
          <p className="min-w-0 truncate text-sm font-semibold text-foreground">DSA</p>
        ) : null}
      </div>

      <nav
        className={cn(
          'flex min-h-0 flex-col gap-1',
          isRail ? 'flex-1 overflow-y-auto overscroll-contain' : 'flex-1 gap-1.5',
        )}
        aria-label={t('layout.mainNav')}
      >
        {navItems.map(({ key, label, labelKey, to, icon: Icon, exact, badge }) => {
          const itemLabel = labelKey ? t(labelKey) : (label ?? key);
          return (
          <NavLink
            key={key}
            to={to}
            end={exact}
            onClick={onNavigate}
            aria-label={itemLabel}
            className={({ isActive }) =>
              cn(
                itemInteractiveClass,
                isActive ? itemActiveClass : ''
              )
            }
          >
            {({ isActive }) => (
              <>
                <Icon className={cn(itemIconClass, isActive ? 'text-[var(--nav-icon-active)]' : 'text-current')} />
                {!collapsed ? <span className={itemLabelClass}>{itemLabel}</span> : null}
                {badge === 'completion' && completionBadge ? (
                  <StatusDot
                    tone="info"
                    data-testid="chat-completion-badge"
                    className={cn(
                      'absolute border-2 border-background shadow-[0_0_10px_var(--nav-indicator-shadow)]',
                      isRail ? 'right-1 top-1' : collapsed ? 'right-2 top-2' : 'right-3',
                    )}
                    aria-label={t('layout.newChatMessage')}
                  />
                ) : null}
              </>
            )}
          </NavLink>
          );
        })}
      </nav>

      <div className={cn('shrink-0 space-y-1', isRail ? 'mt-2 border-t border-border/40 pt-2' : 'mt-3')}>
        <ThemeToggle
          variant={isRail ? 'rail' : 'nav'}
          collapsed={collapsed}
          wrapperClassName="w-full"
          triggerClassName={itemInteractiveClass}
          triggerActiveClassName={itemActiveClass}
          iconClassName={itemIconClass}
          labelClassName={itemLabelClass}
        />
        {/* <UiLanguageToggle
          variant={isRail ? 'rail' : 'nav'}
          collapsed={collapsed}
          wrapperClassName="w-full"
          triggerClassName={itemInteractiveClass}
          triggerActiveClassName={itemActiveClass}
          iconClassName={itemIconClass}
          labelClassName={itemLabelClass}
        /> */}
        {authEnabled ? (
          <button
            type="button"
            onClick={() => setShowLogoutConfirm(true)}
            className={itemInteractiveClass}
          >
            <LogOut className={itemIconClass} />
            {!collapsed ? <span className={itemLabelClass}>{t('layout.logout')}</span> : null}
          </button>
        ) : null}
      </div>

      <ConfirmDialog
        isOpen={showLogoutConfirm}
        title={t('layout.logoutTitle')}
        message={t('layout.logoutMessage')}
        confirmText={t('layout.logoutConfirm')}
        cancelText={t('common.cancel')}
        isDanger
        onConfirm={() => {
          setShowLogoutConfirm(false);
          onNavigate?.();
          void logout();
        }}
        onCancel={() => setShowLogoutConfirm(false)}
      />
    </div>
  );
};
