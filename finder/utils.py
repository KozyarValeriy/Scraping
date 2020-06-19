def async_send_mail(msg):
    with app.app_context():
        mail.send(msg)


def send_mail(subject, recipient, body):
    msg = Message(subject, sender=app.config['MAIL_DEFAULT_SENDER'], recipients=[recipient])
    msg.body = body
    thread = Thread(target=async_send_mail, args=(msg,))
    thread.start()
    return thread