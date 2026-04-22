import type React from 'react';
import type { MatchedSkill, ReportLanguage } from '../../types/analysis';
import { Badge, Card } from '../common';
import { DashboardPanelHeader } from '../dashboard';
import { getReportText, normalizeReportLanguage } from '../../utils/reportLanguage';

interface ReportMatchedSkillsProps {
  matchedSkills?: MatchedSkill[];
  language?: ReportLanguage;
}

type ConfidenceTone = 'default' | 'success' | 'warning' | 'info';

const CONFIDENCE_TONE_MAP: Record<string, ConfidenceTone> = {
  '高': 'success',
  '中': 'info',
  '低': 'warning',
  high: 'success',
  medium: 'info',
  low: 'warning',
};

const getConfidenceTone = (confidence?: string): ConfidenceTone => {
  if (!confidence) return 'default';
  return CONFIDENCE_TONE_MAP[confidence.trim().toLowerCase()] ?? CONFIDENCE_TONE_MAP[confidence.trim()] ?? 'default';
};

/**
 * 命中交易技能展示 —— 把 LLM 在本次分析中触发的 AGENT_SKILLS 展示为卡片条目。
 * 首条（置信度最高的）自动标记为主命中。
 */
export const ReportMatchedSkills: React.FC<ReportMatchedSkillsProps> = ({
  matchedSkills,
  language = 'zh',
}) => {
  const reportLanguage = normalizeReportLanguage(language);
  const text = getReportText(reportLanguage);

  if (!matchedSkills || matchedSkills.length === 0) {
    return null;
  }

  return (
    <Card variant="bordered" padding="md" className="home-panel-card">
      <DashboardPanelHeader
        eyebrow={text.matchedSkillsEyebrow}
        title={text.matchedSkillsTitle}
        className="mb-3"
      />

      <ul className="flex flex-col gap-2.5">
        {matchedSkills.map((skill, idx) => {
          const isPrimary = idx === 0;
          const displayName = skill.name || skill.id || '—';
          const tone = getConfidenceTone(skill.confidence);
          const conditions = (skill.matchedConditions || []).filter((c) => c && c.trim());

          return (
            <li
              key={`${skill.id ?? 'skill'}-${idx}`}
              className={[
                'rounded-lg border px-3 py-2.5 transition-colors',
                isPrimary
                  ? 'border-success/35 bg-success/8'
                  : 'border-border/55 bg-elevated/40',
              ].join(' ')}
            >
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-xs font-mono text-muted-text">#{idx + 1}</span>
                {isPrimary ? (
                  <Badge variant="success" size="sm" glow>
                    ⭐ {text.matchedSkillsPrimary}
                  </Badge>
                ) : null}
                <span className="text-sm font-semibold text-foreground">{displayName}</span>
                {skill.id ? (
                  <code className="home-accent-chip px-1.5 py-0.5 font-mono text-[11px]">
                    {skill.id}
                  </code>
                ) : null}
                {skill.confidence ? (
                  <Badge variant={tone} size="sm">
                    {text.matchedSkillsConfidence}: {skill.confidence}
                  </Badge>
                ) : null}
              </div>

              {skill.reason ? (
                <p className="mt-1.5 text-xs leading-relaxed text-secondary-text">
                  <span className="mr-1 text-muted-text">{text.matchedSkillsReason}:</span>
                  {skill.reason}
                </p>
              ) : null}

              {conditions.length > 0 ? (
                <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                  <span className="text-[11px] text-muted-text">
                    {text.matchedSkillsConditions}:
                  </span>
                  {conditions.map((cond, condIdx) => (
                    <span
                      key={`${cond}-${condIdx}`}
                      className="inline-flex items-center rounded border border-border/45 bg-base/60 px-1.5 py-0.5 text-[11px] text-secondary-text"
                    >
                      {cond}
                    </span>
                  ))}
                </div>
              ) : null}
            </li>
          );
        })}
      </ul>
    </Card>
  );
};
