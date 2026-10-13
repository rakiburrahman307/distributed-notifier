import { NotificationChannel } from '../types/notification.js';

export interface TemplateContent {
  title: string;
  body: string;
}

export interface NotificationTemplate {
  id: string;
  title: string;
  body: string;
  defaultChannels?: NotificationChannel[];
  data?: Record<string, unknown>;
  locales?: Record<string, TemplateContent>; // e.g. { bn: { title: "...", body: "..." } }
  defaultLocale?: string;
}

export class TemplateEngine {
  private readonly templates = new Map<string, NotificationTemplate>();

  public register(template: NotificationTemplate): void {
    if (!template.id) {
      throw new Error('Template must have a non-empty ID');
    }
    this.templates.set(template.id, template);
  }

  public registerBatch(templates: NotificationTemplate[]): void {
    for (const t of templates) {
      this.register(t);
    }
  }

  public get(id: string): NotificationTemplate | undefined {
    return this.templates.get(id);
  }

  public render(
    templateId: string,
    variables: Record<string, unknown> = {},
    locale?: string
  ): { title: string; body: string; defaultChannels?: NotificationChannel[]; data?: Record<string, unknown>; resolvedLocale?: string } {
    const template = this.templates.get(templateId);
    if (!template) {
      throw new Error(`Notification template with ID "${templateId}" not found`);
    }

    // Resolve localized content
    let rawTitle = template.title;
    let rawBody = template.body;
    let resolvedLocale = template.defaultLocale || 'en';

    if (locale && template.locales && template.locales[locale]) {
      rawTitle = template.locales[locale].title;
      rawBody = template.locales[locale].body;
      resolvedLocale = locale;
    }

    return {
      title: this.interpolate(rawTitle, variables),
      body: this.interpolate(rawBody, variables),
      defaultChannels: template.defaultChannels,
      data: template.data,
      resolvedLocale,
    };
  }

  /**
   * Evaluates template string with:
   * 1. Conditionals: {{#if path}}...{{else}}...{{/if}}
   * 2. Raw unescaped output: {{{path}}}
   * 3. HTML-escaped output: {{path}}
   * 4. Nested object paths: user.profile.name
   */
  public interpolate(text: string, variables: Record<string, unknown> = {}): string {
    // 1. Process conditionals: {{#if path}}then_block{{else}}else_block{{/if}}
    const ifRegex = /{{\s*#if\s+([a-zA-Z0-9_.]+)\s*}}([\s\S]*?)(?:{{\s*else\s*}}([\s\S]*?))?{{\s*\/if\s*}}/g;
    let processed = text.replace(ifRegex, (_, path, thenBlock, elseBlock) => {
      const val = this.resolvePath(variables, path);
      const isTruthy = Boolean(val && (!Array.isArray(val) || val.length > 0));
      return isTruthy ? thenBlock : (elseBlock || '');
    });

    // 2. Process raw unescaped placeholders: {{{path}}}
    const rawRegex = /{{{\s*([a-zA-Z0-9_.]+)\s*}}}/g;
    processed = processed.replace(rawRegex, (_, path) => {
      const val = this.resolvePath(variables, path);
      return val !== undefined && val !== null ? String(val) : '';
    });

    // 3. Process escaped placeholders: {{path}}
    const varRegex = /{{\s*([a-zA-Z0-9_.]+)\s*}}/g;
    processed = processed.replace(varRegex, (_, path) => {
      const val = this.resolvePath(variables, path);
      if (val === undefined || val === null) {
        return `{{${path}}}`; // Preserve missing variable
      }
      return this.escapeHtml(String(val));
    });

    return processed;
  }

  private resolvePath(obj: Record<string, unknown>, path: string): unknown {
    if (!obj || typeof obj !== 'object') return undefined;
    if (path in obj) return obj[path];

    const parts = path.split('.');
    let current: any = obj;

    for (const part of parts) {
      if (current === null || current === undefined) {
        return undefined;
      }
      current = current[part];
    }

    return current;
  }

  private escapeHtml(str: string): string {
    return str
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  public has(id: string): boolean {
    return this.templates.has(id);
  }
}
