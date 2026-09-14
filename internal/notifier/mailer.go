package notifier

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"gopkg.in/gomail.v2"

	"anomaly-monitor/internal/config"
	"anomaly-monitor/internal/model"
)

// Mailer отправляет email-уведомления через SMTP.
type Mailer struct {
	cfg *config.Config
	log *zap.Logger
}

func NewMailer(cfg *config.Config, log *zap.Logger) *Mailer {
	return &Mailer{cfg: cfg, log: log}
}

// SendAlert формирует и отправляет письмо об аномалии.
// recipients — строка адресов через запятую.
// Возвращает nil, если письмо отправлено либо осознанно пропущено
// (SMTP не настроен), и ошибку, если отправка не удалась.
func (m *Mailer) SendAlert(ev model.AccessEvent, threshold float64, recipients string) error {
	if m.cfg.SMTPUser == "" || recipients == "" {
		m.log.Debug("SMTP не настроен или получатели не заданы, письмо пропущено")
		return nil
	}

	addrs := splitRecipients(recipients)
	if len(addrs) == 0 {
		return nil
	}

	subject := fmt.Sprintf("[ANOMALY] User %d — score %.2f (порог %.2f)",
		ev.UserID, ev.AnomalyScore, threshold)

	body := m.buildBody(ev, threshold)

	msg := gomail.NewMessage()
	msg.SetHeader("From", m.cfg.SMTPFrom)
	msg.SetHeader("To", addrs...)
	msg.SetHeader("Subject", subject)
	msg.SetBody("text/html", body)

	d := gomail.NewDialer(
		m.cfg.SMTPHost,
		m.cfg.SMTPPort,
		m.cfg.SMTPUser,
		m.cfg.SMTPPassword,
	)

	// Результат отправки журналирует вызывающая сторона (service.sendAlerts),
	// чтобы запись в журнале соответствовала фактическому исходу.
	if err := d.DialAndSend(msg); err != nil {
		return fmt.Errorf("отправка письма на %s: %w", recipients, err)
	}
	return nil
}

func (m *Mailer) buildBody(ev model.AccessEvent, threshold float64) string {
	accessStr := "разрешён"
	if ev.AccessResult == 0 {
		accessStr = "отказан"
	}

	actionColor := "#27ae60"
	switch ev.BlockAction {
	case "monitor":
		actionColor = "#e67e22"
	case "block":
		actionColor = "#e74c3c"
	}

	return fmt.Sprintf(`<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px">

<div style="background:#e74c3c;color:#fff;padding:16px 20px;border-radius:8px 8px 0 0">
  <h2 style="margin:0">⚠ Обнаружена аномалия доступа</h2>
  <p style="margin:4px 0 0;opacity:.85">Система мониторинга АСУ</p>
</div>

<div style="border:1px solid #ddd;border-top:none;padding:20px;border-radius:0 0 8px 8px">
  <table style="width:100%%;border-collapse:collapse">
    <tr style="background:#f8f9fa">
      <td style="padding:10px 12px;font-weight:bold;width:40%%">Пользователь (ID)</td>
      <td style="padding:10px 12px">%d</td>
    </tr>
    <tr>
      <td style="padding:10px 12px;font-weight:bold">IP-адрес</td>
      <td style="padding:10px 12px">%s</td>
    </tr>
    <tr style="background:#f8f9fa">
      <td style="padding:10px 12px;font-weight:bold">Задача (ID)</td>
      <td style="padding:10px 12px">%d</td>
    </tr>
    <tr>
      <td style="padding:10px 12px;font-weight:bold">Дата и время</td>
      <td style="padding:10px 12px">%s</td>
    </tr>
    <tr style="background:#f8f9fa">
      <td style="padding:10px 12px;font-weight:bold">Доступ</td>
      <td style="padding:10px 12px">%s</td>
    </tr>
    <tr>
      <td style="padding:10px 12px;font-weight:bold">Anomaly score</td>
      <td style="padding:10px 12px">
        <span style="background:#e74c3c;color:#fff;padding:3px 10px;border-radius:12px;font-weight:bold">
          %.4f
        </span>
        <span style="color:#888;font-size:13px">(порог: %.2f)</span>
      </td>
    </tr>
    <tr style="background:#f8f9fa">
      <td style="padding:10px 12px;font-weight:bold">Рекомендация</td>
      <td style="padding:10px 12px">
        <span style="background:%s;color:#fff;padding:3px 10px;border-radius:12px;font-weight:bold">
          %s
        </span>
      </td>
    </tr>
  </table>

  <p style="color:#888;font-size:12px;margin-top:20px;border-top:1px solid #eee;padding-top:12px">
    Это автоматическое уведомление системы обнаружения аномалий.<br>
    Для просмотра подробностей откройте дашборд мониторинга.
  </p>
</div>

</body></html>`,
		ev.UserID,
		ev.IP,
		ev.TaskID,
		ev.AccessDate.Format("02.01.2006 15:04:05"),
		accessStr,
		ev.AnomalyScore,
		threshold,
		actionColor,
		ev.BlockAction,
	)
}

func splitRecipients(s string) []string {
	var result []string
	for _, addr := range strings.Split(s, ",") {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			result = append(result, addr)
		}
	}
	return result
}
