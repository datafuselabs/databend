import React, { ReactNode, useEffect } from 'react';
import ReactDOM from 'react-dom';
import styles from './styles.module.scss';
import clsx from 'clsx';
interface IProps {
  visible: boolean;
  width?: number;
  children: ReactNode;
  className?: string;
  onClose?: () => void;
}
const CommonModal = ({ visible, width, children, className, onClose }: IProps) => {
  useEffect(() => {
    if (visible) {
      document.body.style.overflow = 'hidden';
    }
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onClose?.();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => {
      document.body.style.overflow = 'visible';
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, [visible, onClose]);

  if (!visible) {
    return null;
  }

  return ReactDOM.createPortal(
    <div className={clsx(styles.modalOverlay, className)} onClick={onClose}>
      <div className={styles.modalContent} style={{ width: width+"px" }} onClick={(e) => e.stopPropagation()}>
        {children}
      </div>
    </div>,
    document.body
  );
};
CommonModal.defaultOptions = {
  width: 560
}

export default CommonModal;
