import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebar: SidebarsConfig = {
  apisidebar: [
    {
      type: "doc",
      id: "api-reference/rest/labor-performance-api",
    },
    {
      type: "category",
      label: "standards",
      link: {
        type: "doc",
        id: "api-reference/rest/standards",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/define-standard",
          label: "Define (or revise) the engineered labor standard for a TaskType",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-standard",
          label: "Get the currently-active labor standard for a TaskType",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "performance",
      link: {
        type: "doc",
        id: "api-reference/rest/performance",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-associate-scorecard",
          label: "Get one associate's performance scorecard",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-task-type-performance",
          label: "Get fleet-wide (all-associates) performance for a TaskType",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "utilization",
      link: {
        type: "doc",
        id: "api-reference/rest/utilization",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-task-type-utilization",
          label: "Get fleet-wide idle-gap / utilization for a TaskType over a trailing window",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-associate-utilization",
          label: "Get one associate's idle-gap / utilization over a trailing window",
          className: "api-method get",
        },
      ],
    },
    {
      type: "category",
      label: "health",
      link: {
        type: "doc",
        id: "api-reference/rest/health",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/get-healthz",
          label: "Liveness probe",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-readyz",
          label: "Readiness probe",
          className: "api-method get",
        },
      ],
    },
  ],
};

export default sidebar.apisidebar;
