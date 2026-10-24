import type {ReactNode} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

import HomepageFeatures from '@site/src/components/HomepageFeatures';
import styles from './index.module.css';

function StudyDisclaimer() {
  return (
    <div
      style={{
        background: '#fef3c7',
        color: '#78350f',
        textAlign: 'center',
        padding: '0.6rem 1rem',
        fontSize: '0.9rem',
        borderBottom: '1px solid #f59e0b',
      }}>
      ⚠️ <strong>Study project</strong> — an educational DDD exercise
      following real industry-standard patterns (WMS/WES/WCS, CloudEvents,
      RFC 7807, hexagonal architecture). Not a production system. Not
      affiliated with, endorsed by, or representative of Manhattan
      Associates, Blue Yonder, or any other company.
    </div>
  );
}

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero', styles.heroBanner)}>
      <StudyDisclaimer />
      <div className="container">
        <p className={styles.eyebrow}>
          warehouse-systems · Supporting subdomain · Kafka consumer
        </p>
        <Heading as="h1" className={styles.heroTitle}>
          {siteConfig.title}
        </Heading>
        <p className={styles.heroSubtitle}>{siteConfig.tagline}</p>
        <p className={styles.heroLead}>
          Engineered labor standards ("a PICK should take 45s") scored
          against actual completions consumed from fulfillment-execution's
          TaskCompleted event — the fleet's answer to Manhattan Active Labor
          Management and Blue Yonder Workforce &amp; Labor Management.
        </p>
        <div className={styles.buttons}>
          <Link className="button button--primary button--lg" to="/docs/overview">
            Read the docs
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="/docs/api-reference/rest/labor-performance-api">
            API Reference
          </Link>
          <Link className="button button--secondary button--lg" to="/docs/adr">
            ADRs
          </Link>
        </div>
      </div>
    </header>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="Documentation for the Labor Performance bounded context: engineered labor standards and actual-vs-standard performance scoring.">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <section className={styles.invariant}>
          <div className="container">
            <blockquote className={styles.invariantQuote}>
              This context flags a gap. It never decides what to do about
              it.
            </blockquote>
            <p className={styles.invariantCaption}>
              No automatic coaching, no automatic pay-for-performance — the
              same "visibility, not enforcement" discipline
              workforce-management applies to PathUnderstaffed.{' '}
              <Link to="/docs/business-context/domain-vision">
                Why it reads that way →
              </Link>
            </p>
          </div>
        </section>
      </main>
    </Layout>
  );
}
