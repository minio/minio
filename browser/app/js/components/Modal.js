import React from "react"
import { CSSTransition } from "react-transition-group"

export class Modal extends React.Component {
  render() {
    const {
      className,
      modalShow,
      showClose,
      modalClose,
      modalIcon,
      modalTitle,
      modalSubtitle,
      children
    } = this.props
    const modalProps = {
      mountOnEnter: true,
      unmountOnExit: true,
      timeout: {
        enter: 10,
        exit: 300
      },
      classNames: {
        enter: "modal--enter",
        enterDone: "modal--enter-done",
        exit: "modal--exit",
        exitDone: "modal--exit-done"
      }
    }
    let modalClassName = className || ""

    return (
      <CSSTransition in={modalShow} {...modalProps}>
        {state => (
          <div className={"modal " + modalClassName}>
            <div className="modal__content">
              {showClose && <i className="modal__close" onClick={modalClose} />}

              {modalIcon && (
                <div className="modal__icon">
                  <img src={modalIcon} alt="" />
                </div>
              )}

              {modalTitle && <div className="modal__title">{modalTitle}</div>}

              {modalSubtitle && (
                <div className="modal__sub-title">{modalSubtitle}</div>
              )}

              {children}
            </div>
          </div>
        )}
      </CSSTransition>
    )
  }
}

export default Modal
